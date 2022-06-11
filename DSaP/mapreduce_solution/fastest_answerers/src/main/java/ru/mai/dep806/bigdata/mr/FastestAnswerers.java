package ru.mai.dep806.bigdata.mr;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Stream;

import static ru.mai.dep806.bigdata.mr.SequenceFileUtils.toSequenceString;

/**
 * Задание: Найти пользователей, которые быстрее всех правильно отвечают на вопросы на StackOverflow
 *
 * По сути это раелизация следующей серии запросов в HIVE'е, только на MapReduce
 *
 * -- Создаем вспомогательную таблицу со всеми вопросами
 * CREATE TABLE questions AS (
 *   SELECT
 *     *
 *   FROM
 *     default.posts
 *   WHERE
 *     posttypeid = 1
 * );
 *
 * -- Создаем вспомогательную таблицу со всеми ответами
 * CREATE TABLE answers AS (
 *   SELECT
 *     *
 *   FROM
 *     default.posts
 *   WHERE
 *     posttypeid = 2
 * );
 *
 * -- Формируем таблицу, в которой лежат id пользователя ответившего правильно на вопрос и время между вопросом и ответом в секундах для тех вопросов, для которых:
 * -- + задавший и ответивший пользователь отличаются и время, прошедшее с момента создания вопроса, до момента создания правильного ответа, превышает 5 минут
 * -- или
 * -- + задавший и ответивший пользователь совпадают и время, прошедшее с момента создания вопроса, до момента создания правильного ответа, превышает 1 час
 * -- Делается это для того, чтобы отсеять случаи "накрутки" пользователями своей статистики правильных ответов
 * CREATE TABLE cheat_filtered AS (
 *   SELECT
 *     a.owneruserid AS answerer_id,
 *     unix_timestamp(a.creationdate, "yyyy-MM-dd'T'HH:mm:ss.SSS") - unix_timestamp(q.creationdate, "yyyy-MM-dd'T'HH:mm:ss.SSS") AS seconds_to_answer
 *   FROM
 *     questions q
 *     JOIN answers a ON (q.acceptedanswerid = a.id)
 *   WHERE
 *     (q.owneruserid <> a.owneruserid
 *       AND unix_timestamp(a.creationdate, "yyyy-MM-dd'T'HH:mm:ss.SSS") - unix_timestamp(q.creationdate, "yyyy-MM-dd'T'HH:mm:ss.SSS") > (60 * 5))
 *   OR (q.owneruserid = a.owneruserid
 *       AND unix_timestamp(a.creationdate, "yyyy-MM-dd'T'HH:mm:ss.SSS") - unix_timestamp(q.creationdate, "yyyy-MM-dd'T'HH:mm:ss.SSS") > (60 * 60))
 * );
 *
 * -- создается временная таблица, в которой содержится id отвечавшего пользователя, среднее время между созданием вопроса и ответом этого пользователя,
 * -- и общее число вопросов, на которые правильно ответил пользователь
 * CREATE temporary TABLE temp AS (
 *   select
 *     answerer_id,
 *     avg(seconds_to_answer) AS avg_answer_time,
 *     count(*) AS total_answers
 *   FROM
 *     cheat_filtered
 *   GROUP BY
 *     answerer_id
 *   HAVING
 *     count(*) > 3
 * );
 *
 * -- таблица, в которой содержится id отвечающего, его никнейм, среднее время между созданием вопроса и ответом этого пользователя,
 * -- и общее число вопросов, на которые правильно ответил пользователь
 * select
 *   t.answerer_id,
 *   u.displayname,
 *   t.avg_answer_time,
 *   t.total_answers
 * FROM
 *   temp AS t
 *   JOIN default.users AS u ON (t.answerer_id = u.id)
 * ORDER BY
 *   t.avg_answer_time;
 *

 * Запуск:
 * hadoop jar fastest_answerers-1.0-SNAPSHOT.jar ru.mai.dep806.bigdata.mr.FastestAnswerers /user/stud/stackoverflow/landing/Posts /user/stud/stackoverflow/landing/Users <output_path> <run_filters> <run_join> <run_aggregation> <run_aggregate_user_join> <run_top_N> <top_N>
 * где <run_*> должны быть true или false. Каждый такой аргумент указывает программе нужно ли запускать соответствующую стадию обработки, или использовать данные с прошлых запусков.
 * <top_N> -- число быстрейших "отвечателей", которое необходимо вывести
 */

public class FastestAnswerers extends Configured implements Tool {
    private static final String[] postsFields = new String[] {
            "Id", "PostTypeId", "AcceptedAnswerId", "ParentId" , "CreationDate", "DeletionDate",
            "Score", "ViewCount", "Body" , "OwnerUserId", "OwnerDisplayName", "LastEditorUserId",
            "LastEditorDisplayName", "LastEditDate", "LastActivityDate", "Title", "Tags", "AnswerCount",
            "CommentCount", "FavoriteCount", "ClosedDate", "CommunityOwnedDate"
    };

    // Базовый класс-маппер для фильтрации постов из файла Posts.xml по некоторому условию
    abstract static class PostsFilterMapper extends Mapper<Object, Text, NullWritable, Text> {
        protected abstract boolean condition(Map<String, String> row);
        private final Text outValue = new Text();

        public void map(Object key, Text value, Mapper.Context context)
                throws IOException, InterruptedException {

            Map<String, String> row = XmlUtils.parseXmlRow(value.toString());

            if (condition(row)) {
                outValue.set(SequenceFileUtils.toSequenceString(row, postsFields));
                context.write(NullWritable.get(), outValue);
            }
        }
    }

    // Специализация базового класса-маппера для фильтрации вопросов из всех постов
    private static class QuestionsMapper extends PostsFilterMapper {
        @Override
        protected boolean condition(Map<String, String> row) {
            return "1".equals(row.get("PostTypeId"));
        }
    }

    // Специализация базового класса-маппера для фильтрации ответов из всех постов
    private static class AnswersMapper extends PostsFilterMapper {
        @Override
        protected boolean condition(Map<String, String> row) {
            return "2".equals(row.get("PostTypeId"));
        }
    }

    // Функция для рассчета разницы между моментами времени в секундах. По-хорошему должна работать как unix_timestamp(d2) - unix_timestamp(d1) в HIVE,
    // но по факту иногда отличается на 1, что в свою очередь приводит к разному среднему времени ответа, из-за чего в финальном топе "отвечателей"
    // результаты MapReduce несколько отличаются от HIVE'овских (в основном меняется порядок пользователей в топе в топе)
    private static long getTimeDifferenceInSeconds(Date d1, Date d2){
        return (d2.getTime() - d1.getTime()) / 1000;
    }

    // Сериализуемый класс для передачи строки из маппера в редюсер при операции join, в котором хранится строка из маппера
    // + тип записи (т.е. из какой таблицы была взята строчка), для последующего разделения записей в Reducer'е
    private static class TextWithType implements Writable {
        public TextWithType() {
            this(RecordType.NA);
        }

        TextWithType(RecordType recordType) {
            this.recordType = recordType;
            this.record = new Text();
        }

        private RecordType recordType;
        private final Text record;

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(recordType.ordinal());
            record.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            recordType = RecordType.values()[in.readInt()];
            record.readFields(in);
        }

        RecordType getRecordType() {
            return recordType;
        }

        Text getRecord() {
            return record;
        }
    }

    // Тип записи для помещения в TextWithType, для определения в редюсере какой таблицы из соединяемых строка принадлежала
    enum RecordType { NA, Answer, Question }

    // Базовый класс-маппер для соединения таблиц содержащих посты по ключу
    private abstract static class SequenceFileJoinMapper extends Mapper<Object, Text, LongWritable, TextWithType> {
        private final LongWritable outKey = new LongWritable();
        private final TextWithType outValue = new TextWithType(getRecordType());

        // метод для получения типа-записи
        protected abstract RecordType getRecordType();

        // метод для получения значения ключа по которому производится соединение
        protected abstract String getKeyString(Map<String, String> row);

        // метод для получеия имен полей в соединяемой таблице
        protected abstract String[] getFieldNames();

        String[] fieldNames = getFieldNames();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> row = SequenceFileUtils.parseFields(value.toString(), fieldNames);

            String keyString = getKeyString(row);

            if (StringUtils.isNotBlank(keyString)) {
                outKey.set(Long.parseLong(keyString));
                outValue.getRecord().set(toSequenceString(row, fieldNames));
                context.write(outKey, outValue);
            }
        }
    }

    // Класс-маппер для для объединение таблицы Questions с чем-то другим по accepted_answer_id ответа
    private static class QuestionsAcceptedAnswerIdJoinMapper extends SequenceFileJoinMapper {
        @Override
        protected RecordType getRecordType() {
            return RecordType.Question;
        }

        @Override
        protected String getKeyString(Map<String, String> row) {
            return row.get("AcceptedAnswerId");
        }

        @Override
        protected String[] getFieldNames() {
            return postsFields;
        }
    }

    // Класс-маппер для для объединение таблицы Answers с чем-то другим по id ответа
    private static class AnswerIdJoinMapper extends SequenceFileJoinMapper {
        @Override
        protected RecordType getRecordType() {
            return RecordType.Answer;
        }

        @Override
        protected String getKeyString(Map<String, String> row) {
            return row.get("Id");
        }

        @Override
        protected String[] getFieldNames() {
            return postsFields;
        }
    }

    // Редюсер для операции объединения таблиц Questions и Answers по условию Questions.accepted_answer_id = Answers.id.
    // Т.о. в каждой строчке получаем информацию о вопросе и о правильном и честном (не для накрутки рейтингов) ответе на этот вопрос.
    // Критерий честности ответа см. ниже в методе reduce.
    static class JoinReducer extends Reducer<LongWritable, TextWithType, NullWritable, Text> {

        private final Text outValue = new Text();
        private final StringBuilder buffer = new StringBuilder();

        private final static SimpleDateFormat dateFormat;

        static {
            dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
            dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        }

        @Override
        protected void reduce(LongWritable key, Iterable<TextWithType> values, Context context) throws IOException, InterruptedException {

            List<String> questions = new ArrayList<>();
            List<String> answers = new ArrayList<>();

            // Распределим значения по типам строк в соотв. списки
            for (TextWithType value : values) {
                String strValue = value.getRecord().toString();
                switch (value.getRecordType()) {
                    case Question:
                        questions.add(strValue);
                        break;
                    case Answer:
                        answers.add(strValue);
                        break;
                    default:
                        throw new IllegalStateException("Unknown type: " + value.getRecordType());
                }
            }

            try{
                if (questions.size() > 0 && answers.size() > 0) {
                    for (String question : questions) {
                        for (String answer : answers) {

                            Map<String, String> questionFields = SequenceFileUtils.parseFields(question, postsFields);
                            Map<String, String> answerFields = SequenceFileUtils.parseFields(answer, postsFields);

                            String questionCreationDateString = questionFields.get("CreationDate");
                            String answerCreationDateString = answerFields.get("CreationDate");

                            String questionOwnerUserId = questionFields.get("OwnerUserId");
                            String answerOwnerUserId = answerFields.get("OwnerUserId");

                            Date questionDate = dateFormat.parse(questionCreationDateString);
                            Date answerDate = dateFormat.parse(answerCreationDateString);

                            long seconds = getTimeDifferenceInSeconds(questionDate, answerDate);

                            boolean selfAnswer = questionOwnerUserId.equals(answerOwnerUserId);

                            if(StringUtils.isBlank(questionOwnerUserId) || StringUtils.isBlank(answerOwnerUserId))
                                continue;

                            // После join'а строчек сразу же отфильтровываем неудовлетворяющие следующему условию:
                            // Считаем честным ответ на вопрос, если пользователь отвечает не на свой же вопрос более чем через 5 минут
                            // либо если пользователь отвечает на свой же вопрос через час, все обдумав, прогуглив и т.д. и т.п.
                            boolean condition = (!selfAnswer && seconds > 60 * 5) || (selfAnswer && seconds > 60 * 60);

                            if(condition){
                                buffer.setLength(0);
                                buffer.append(question).append(answer);
                                buffer.append(seconds);
                                buffer.append(SequenceFileUtils.FIELD_SEPARATOR);

                                outValue.set(buffer.toString());
                                context.write(NullWritable.get(), outValue);
                            }
                        }
                    }
                }
            }
            catch (ParseException ex){
                throw new IOException("Date parsing failed!");
            }
        }
    }

    // Класс содержащий аггрегаты (среднее время ответа на вопрос, общее число ответов на вопрос) для данного конкретного пользователя.
    public static class Stats implements Writable {
        double averageAnswerTime;
        int totalAnswersCount;

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeDouble(averageAnswerTime);
            out.writeInt(totalAnswersCount);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            averageAnswerTime = in.readDouble();
            totalAnswersCount = in.readInt();
        }

        double getAverageAnswerTime(){
            return averageAnswerTime;
        }

        void setAverageAnswerTime(double value){
            averageAnswerTime = value;
        }

        int getTotalAnswersCount(){
            return totalAnswersCount;
        }

        void setTotalAnswersCount(int value){
            totalAnswersCount = value;
        }
    }

    // Класс-маппер для аггрегирования среднего время ответа на вопрос, общего числа ответов на вопрос.
    // Для каждого вопроса кладет в Stats общее число 1, и время в секундах между ответом на данный конкретный вопрос
    public static class AggregationMapper extends Mapper<Object, Text, Text, Stats> {
        protected static final String[] fieldNames;
        private final static SimpleDateFormat dateFormat;

        static{
            fieldNames = Stream.concat(Stream.concat(Arrays.stream(postsFields).map(x -> "Question" + x), Arrays.stream(postsFields).map(x -> "Answer" + x)), Stream.of("TimeToAnswerInSeconds")).toArray(String[]::new);

            dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
            dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        }

        private final Text outKey = new Text();
        private final Stats outValue = new Stats();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> row = SequenceFileUtils.parseFields(value.toString(), fieldNames);

            String keyString = row.get("AnswerOwnerUserId");

            String questionCreationDateString = row.get("QuestionCreationDate");
            String answerCreationDateString = row.get("AnswerCreationDate");

            Date questionDate, answerDate;
            try{
                questionDate = dateFormat.parse(questionCreationDateString);
                answerDate = dateFormat.parse(answerCreationDateString);
            }
            catch (ParseException ex){
                throw new IOException("Date parsing failed!");
            }

            long seconds = getTimeDifferenceInSeconds(questionDate, answerDate);

            outKey.set(keyString);
            outValue.setAverageAnswerTime(seconds);
            outValue.setTotalAnswersCount(1);

            context.write(outKey, outValue);
        }
    }

    // Комбайнер для аггрегирования среднего время ответа на вопрос, общего числа ответов на вопрос еще на этапе маппинга.
    // Если вдруг окажется так, что несколько ответов на вопросы от одного пользователя окажутся на одном узле с данной
    // конкретной map-задачей, то оони еще до начала reduce-задачи будут саггрегированы в одну запись. На самом деле, для
    // данной задачи комбайнер, скорее всего, сильной погоды не сделает, потому что в среднем пользователь отвечает на
    // очень небольшое количество вопросов, при том что всего ответов очень много, поэтому вероятность оказаться в одном
    // map-процессе довльно маленькая.
    private static class AggregationCombiner extends Reducer<Text, Stats, Text, Stats> {
        private final Stats result = new Stats();

        @Override
        protected void reduce(Text key, Iterable<Stats> values, Context context) throws IOException, InterruptedException {
            result.setAverageAnswerTime(0);
            result.setTotalAnswersCount(0);

            for (Stats stats : values) {
                int totalAnswersCountBefore = result.getTotalAnswersCount();
                result.setTotalAnswersCount(totalAnswersCountBefore + stats.getTotalAnswersCount());
                result.setAverageAnswerTime((result.getAverageAnswerTime() * totalAnswersCountBefore + stats.getAverageAnswerTime()) / result.getTotalAnswersCount());
            }

            context.write(key, result);
        }
    }

    // Редюсер для аггрегирования среднего время ответа на вопрос, общего числа ответов на вопрос еще на этапе маппинга.
    // Реализует точно такую же логику как и AggregationCombiner, за исключением сохранения результатов в Sequence-формат
    private static class AggregationReducer extends Reducer<Text, Stats, NullWritable, Text> {

        private final Text outValue = new Text();
        private final StringBuilder buffer = new StringBuilder();
        private final Stats result = new Stats();

        @Override
        protected void reduce(Text key, Iterable<Stats> values, Context context) throws IOException, InterruptedException {
            result.setAverageAnswerTime(0);
            result.setTotalAnswersCount(0);

            for (Stats stats : values) {
                int totalAnswersCountBefore = result.getTotalAnswersCount();
                int statsAnswersCount = stats.getTotalAnswersCount();
                result.setTotalAnswersCount(totalAnswersCountBefore + statsAnswersCount);
                result.setAverageAnswerTime((result.getAverageAnswerTime() * totalAnswersCountBefore + stats.getAverageAnswerTime() * statsAnswersCount) / result.getTotalAnswersCount());
            }

            if(result.getTotalAnswersCount() > 3){
                buffer.setLength(0);
                buffer
                        .append(key)
                        .append(SequenceFileUtils.FIELD_SEPARATOR)
                        .append(result.getAverageAnswerTime())
                        .append(SequenceFileUtils.FIELD_SEPARATOR)
                        .append(result.getTotalAnswersCount())
                        .append(SequenceFileUtils.FIELD_SEPARATOR);

                outValue.set(buffer.toString());

                context.write(NullWritable.get(), outValue);
            }
        }
    }


    // Тип записи для помещения в AggregateUsersJoinTextWithType, для определения в редюсере какой таблицы из соединяемых строка принадлежала
    enum AggregateUsersJoinRecordType { NA, Aggregate, User }

    // Сериализуемый класс для передачи строки из маппера в редюсер при операции join, в котором хранится строка из маппера
    // + тип записи (т.е. из какой таблицы была взята строчка), для последующего разделения записей в Reducer'е
    private static class AggregateUsersJoinTextWithType implements Writable {
        public AggregateUsersJoinTextWithType() {
            this(AggregateUsersJoinRecordType.NA);
        }

        AggregateUsersJoinTextWithType(AggregateUsersJoinRecordType recordType) {
            this.recordType = recordType;
            this.record = new Text();
        }

        private AggregateUsersJoinRecordType recordType;
        private final Text record;

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(recordType.ordinal());
            record.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            recordType = AggregateUsersJoinRecordType.values()[in.readInt()];
            record.readFields(in);
        }

        AggregateUsersJoinRecordType getRecordType() {
            return recordType;
        }

        Text getRecord() {
            return record;
        }
    }


    // Интересующие поля из таблицы Users, которые хотим получить в результате после join'а аггрегатов со средним
    // временем ответа с талицей Users
    private static final String[] requiredUserFieldNames = new String[] {
        "DisplayName"
    };

    // Класс-маппер для для объединение таблицы Users с чем-то другим по id пользователя
    private static class UserIdMapper extends Mapper<Object, Text, LongWritable, AggregateUsersJoinTextWithType> {
        private final LongWritable outKey = new LongWritable();
        private final AggregateUsersJoinTextWithType outValue = new AggregateUsersJoinTextWithType(AggregateUsersJoinRecordType.User);

        public void map(Object key, Text value, Mapper.Context context)
                throws IOException, InterruptedException {

            String textString = value.toString();

            Map<String, String> row = XmlUtils.parseXmlRow(textString);

            String keyString = row.get("Id");

            if (StringUtils.isNotBlank(keyString)) {
                outValue.getRecord().set(toSequenceString(row, requiredUserFieldNames));
                outKey.set(Long.parseLong(keyString));
                context.write(outKey, outValue);
            }
        }
    }

    // Интересующие поля из таблицы с аггрегатами, которые хотим получить в результате после join'а аггрегатов со средним
    // временем ответа с талицей Users
    private static final String[] aggregateFieldNames = new String[] {
            "AnswerOwnerUserId", "AverageTimeToAnswerInSeconds",  "TotalAnswersCount"
    };

    // Класс-маппер для для объединение таблицы аггрегатов с чем-то другим по answer_owner_user_id
    private static class AggregateAnswerOwnerUserIdMapper extends Mapper<Object, Text, LongWritable, AggregateUsersJoinTextWithType> {
        private final LongWritable outKey = new LongWritable();
        private final AggregateUsersJoinTextWithType outValue = new AggregateUsersJoinTextWithType(AggregateUsersJoinRecordType.Aggregate);

        public void map(Object key, Text value, Mapper.Context context)
                throws IOException, InterruptedException {

            Map<String, String> row = SequenceFileUtils.parseFields(value.toString(), aggregateFieldNames);

            String keyString = row.get("AnswerOwnerUserId");

            if (StringUtils.isNotBlank(keyString)) {
                outKey.set(Long.parseLong(keyString));
                outValue.getRecord().set(toSequenceString(row, aggregateFieldNames));
                context.write(outKey, outValue);
            }
        }
    }

    // Поля, получаемые в результате соединения таблицы Users с аггрегатами
    private static final String[] aggregateUserJoinResultFieldNames = new String[] {
        "AnswerOwnerUserId", "DisplayName", "AverageTimeToAnswerInSeconds",  "TotalAnswersCount"
    };

    // Редюсер для операции объединения таблицы Users и таблицы с аггрегатами по условию Users,id = Aggregates.answerer_id.
    // Т.о. в каждой строчке получаем статистику по числу правильных ответов на вопросы, среднее время ответа, id ответившего и его имя.
    private static class AggregateUsersJoinReducer extends Reducer<LongWritable, AggregateUsersJoinTextWithType, NullWritable, Text> {

        private final Text outValue = new Text();
        private final StringBuilder buffer = new StringBuilder();

        @Override
        protected void reduce(LongWritable key, Iterable<AggregateUsersJoinTextWithType> values, Context context) throws IOException, InterruptedException {

            List<String> users = new ArrayList<>();
            List<String> aggregates = new ArrayList<>();

            // Распределим значения по типам строк в соотв. списки
            for (AggregateUsersJoinTextWithType value : values) {
                String strValue = value.getRecord().toString();
                switch (value.getRecordType()) {
                    case User:
                        users.add(strValue);
                        break;
                    case Aggregate:
                        aggregates.add(strValue);
                        break;
                    default:
                        throw new IllegalStateException("Unknown type: " + value.getRecordType());
                }
            }

            // Если с обеих сторон есть строки для данного ключа (inner join)
            if (users.size() > 0 && aggregates.size() > 0) {
                for (String user : users) {
                    for (String aggregate : aggregates) {

                        Map<String, String> row = SequenceFileUtils.parseFields(user, requiredUserFieldNames);
                        row.putAll(SequenceFileUtils.parseFields(aggregate, aggregateFieldNames));

                        buffer.setLength(0);
                        for(String fieldName : aggregateUserJoinResultFieldNames){
                            buffer.append(row.get(fieldName)).append(SequenceFileUtils.FIELD_SEPARATOR);
                        }

                        outValue.set(buffer.toString());
                        context.write(NullWritable.get(), outValue);
                    }
                }
            }
        }
    }

    // Маппер для определения топа пользователей, быстрее всего отвечающих на вопросы
    // При помощи упорядоченной мапы накапливает пользователей, в среднем быстрее всего
    // отвечавших на вопросы среди пользователей, попавших ан данный маппер.
    private static class TopNMapper extends Mapper<Object, Text, NullWritable, Text> {
        private final TreeMap<Double, Text> topNMap = new TreeMap<>();

        int topN;

        @Override
        protected void setup(Mapper.Context context) {
            topN = Integer.parseInt(context.getConfiguration().get("topN"));
        }

        @Override
        protected void map(Object key, Text value, Context context) {
            Map<String, String> row = SequenceFileUtils.parseFields(value.toString(), aggregateUserJoinResultFieldNames);

            String reputationString = row.get("AverageTimeToAnswerInSeconds");
            if (reputationString != null) {
                topNMap.put(Double.parseDouble(reputationString), new Text(value));

                // если в мапе записей больше, чем нужно
                if (topNMap.size() > topN) {

                    // удаляем запись с наибольшим значением ключа
                    topNMap.remove(topNMap.lastKey());
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Text value : topNMap.values()) {
                context.write(NullWritable.get(), value);
            }
        }
    }

    // Редюсер для определения топа пользователей, быстрее всего отвечающих на вопросы.
    // Реализует точно такую же логику, как и соответствующий маппер только для лучших пользователей из каждого маппера.
    private static class TopNReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
        private final TreeMap<Double, Text> topNMap = new TreeMap<>();

        int topN;

        @Override
        protected void setup(Reducer.Context context) {
            topN = Integer.parseInt(context.getConfiguration().get("topN"));
        }

        @Override
        protected void reduce(NullWritable key, Iterable<Text> values, Context context) {
            for (Text value : values) {
                Map<String, String> row = SequenceFileUtils.parseFields(value.toString(), aggregateUserJoinResultFieldNames);

                String reputationString = row.get("AverageTimeToAnswerInSeconds");
                if (reputationString != null) {
                    topNMap.put(Double.parseDouble(reputationString), new Text(value));

                    if (topNMap.size() > topN) {
                        topNMap.remove(topNMap.lastKey());
                    }
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Text value : topNMap.values()) {
                context.write(NullWritable.get(), value);
            }
        }
    }

    // функция для удаления папки c HDFS, если она существует
    private static void deleteFolderIfExists (Configuration conf, Path path) throws Exception {
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(path)) {
            fs.delete(path, true);
        }
    }

    // метод для запуска задачи по фильтрации вопросов из таблицы Posts
    public static boolean filterQuestions(Configuration conf, Path inputPath, Path outputPath) throws Exception {
        deleteFolderIfExists(conf, outputPath);

        Job job = Job.getInstance(conf, "Filter questions");
        job.setJarByClass(FastestAnswerers.class);
        job.setMapperClass(QuestionsMapper.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inputPath);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true);
    }

    // метод для запуска задачи по фильтрации ответов из таблицы Posts
    public static boolean filterAnswers(Configuration conf, Path inputPath, Path outputPath) throws Exception {
        deleteFolderIfExists(conf, outputPath);

        Job job = Job.getInstance(conf, "Filter answers");
        job.setJarByClass(FastestAnswerers.class);
        job.setMapperClass(AnswersMapper.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inputPath);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true);
    }

    // метод для запуска задачи по по соединению таблиц Questions и Answers
    boolean joinQuestionsAnswers (Configuration conf, Path questionsPath, Path answersPath, Path joinPath) throws Exception {
        deleteFolderIfExists(conf, joinPath);

        Job job = Job.getInstance(getConf(), "Join Questions and Answers");

        job.setJarByClass(JoinReducer.class);
        job.setReducerClass(JoinReducer.class);

        job.setNumReduceTasks(10);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(TextWithType.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, questionsPath, SequenceFileInputFormat.class, QuestionsAcceptedAnswerIdJoinMapper.class);
        MultipleInputs.addInputPath(job, answersPath, SequenceFileInputFormat.class, AnswerIdJoinMapper.class);

        FileOutputFormat.setOutputPath(job, joinPath);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        return job.waitForCompletion(true);
    }

    // метод для запуска задачи по аггрегации числа правильных ответов пользователя на вопросы и среднего времени ответа
    public static boolean aggregateAverageAnswerTime(Configuration conf, Path inputPath, Path outputPath) throws Exception {
        deleteFolderIfExists(conf, outputPath);

        Job job = Job.getInstance(conf, "StackOverflow average answer time statistics");

        job.setJarByClass(FastestAnswerers.class);
        job.setMapperClass(AggregationMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Stats.class);

        job.setCombinerClass(AggregationCombiner.class);
        job.setReducerClass(AggregationReducer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.setInputPaths(job, inputPath);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true);
    }

    // метод для запуска задачи по соединениию таблицы с аггрегатами с таблицей Users
    boolean joinAggregatesUsers (Configuration conf, Path usersPath, Path aggregatesPath, Path joinPath) throws Exception {
        deleteFolderIfExists(conf, joinPath);

        Job job = Job.getInstance(getConf(), "Join Aggregates and Users");

        job.setJarByClass(AggregateUsersJoinReducer.class);
        job.setReducerClass(AggregateUsersJoinReducer.class);

        job.setNumReduceTasks(10);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(AggregateUsersJoinTextWithType.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, usersPath, TextInputFormat.class, UserIdMapper.class);
        MultipleInputs.addInputPath(job, aggregatesPath, SequenceFileInputFormat.class, AggregateAnswerOwnerUserIdMapper.class);

        FileOutputFormat.setOutputPath(job, joinPath);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        return job.waitForCompletion(true);
    }

    // метод для запуска задачи по нахождению нескольких пользователей, быстрее всех правильно отвечающих на вопросы
    boolean topNAnswerers (Configuration conf, Path inputPath, Path outputPath, int topN) throws Exception {
        deleteFolderIfExists(conf, outputPath);

        Job job = Job.getInstance(conf, "Top N answerers");

        job.setJarByClass(TopNMapper.class);
        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);

        job.setNumReduceTasks(1);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.setInputPaths(job, inputPath);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, outputPath);

        job.getConfiguration().set("topN", Integer.toString(topN));

        return job.waitForCompletion(true);
    }

    private void cleanup(Configuration conf, Path... tempPaths){
        for(Path path : tempPaths){
            try{
                deleteFolderIfExists(conf, path);
            }
            catch (Exception ex){
                System.err.println("Error occurred during cleanup: " + ex.getMessage());
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Path postsPath = new Path(args[0]);
        Path usersPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        System.out.println("paths:");
        System.out.println("posts path: " + postsPath);
        System.out.println("users path: " + usersPath);
        System.out.println("output path: " + outputPath);

        boolean runFilters = Boolean.parseBoolean(args[3]);
        boolean runJoin = Boolean.parseBoolean(args[4]);
        boolean runAggregation = Boolean.parseBoolean(args[5]);
        boolean runUserAggregateJoin = Boolean.parseBoolean(args[6]);
        boolean runTopN = Boolean.parseBoolean(args[7]);

        int topN = Integer.parseInt(args[8]);

        System.out.println("stages to perform:");
        System.out.println("questions and answers retrieving: " + runFilters);
        System.out.println("questions and answers joining: " + runJoin);
        System.out.println("average answer time aggregation: " + runAggregation);
        System.out.println("user and aggregation result join: " + runUserAggregateJoin);
        System.out.println("top N fastest answerers: " + runTopN);
        System.out.println("top N: " + topN);

        Path stagingPath = new Path(outputPath + "_stage");
        Path questionsPath = new Path(stagingPath, "questions");
        Path answersPath = new Path(stagingPath, "answers");
        Path questionsAnswersJoinPath = new Path(stagingPath, "questions_answers_join");
        Path aggregationPath = new Path(stagingPath, "average_answer_time");
        Path usersAggregatesJoinPath = new Path(stagingPath, "users_aggregates_join");

        System.out.println("temporary paths:");
        System.out.println("staging path: " + stagingPath);
        System.out.println("questions path: " + questionsPath);
        System.out.println("answers path: " + answersPath);
        System.out.println("questions and answers join path: " + questionsAnswersJoinPath);
        System.out.println("aggregation path: " + aggregationPath);
        System.out.println("users and aggregations join path: " + usersAggregatesJoinPath);

        Configuration conf = new Configuration();

        if (runFilters){
            if (!filterQuestions(conf, postsPath, questionsPath)){
                System.out.println("Question filtering failed");
                return 1;
            }

            if (!filterAnswers(conf, postsPath, answersPath)){
                System.out.println("Answers filtering failed");
                return 1;
            }
        }

        if(runJoin){
            if (!joinQuestionsAnswers(conf, questionsPath, answersPath, questionsAnswersJoinPath)){
                System.out.println("Questions-answers join failed");
                return 1;
            }
        }

        if(runAggregation){
            if(!aggregateAverageAnswerTime(conf, questionsAnswersJoinPath, aggregationPath)){
                System.out.println("Average answer time aggregation failed");
                return 1;
            }
        }

        if(runUserAggregateJoin){
            if(!joinAggregatesUsers(conf, usersPath, aggregationPath, usersAggregatesJoinPath)){
                System.out.println("Average answer time aggregation failed");
                return 1;
            }
        }

        if(runTopN){
            if(!topNAnswerers(conf, usersAggregatesJoinPath, outputPath, topN)){
                System.out.println("Top 5 answerers failed");
                return 1;
            }
        }

        cleanup(conf, stagingPath, questionsPath, answersPath, questionsAnswersJoinPath, aggregationPath, usersAggregatesJoinPath);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new FastestAnswerers(), args);
        System.exit(result);
    }
}

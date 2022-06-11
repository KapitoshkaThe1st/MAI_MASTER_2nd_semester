-- Задание: Найти пользователей, которые быстрее всех правильно отвечают на вопросы

-- Создаем вспомогательную таблицу со всеми вопросами
CREATE TABLE questions AS (
  SELECT
    * 
  FROM
    default.posts
  WHERE 
    posttypeid = 1
);

-- Создаем вспомогательную таблицу со всеми ответами
CREATE TABLE answers AS (
  SELECT 
    * 
  FROM 
    default.posts
  WHERE 
    posttypeid = 2
);

-- Формируем таблицу, в которой лежат id пользователя ответившего правильно на вопрос и время между вопросом и ответом в секундах для тех вопросов, для которых:
-- + задавший и ответивший пользователь отличаются и время, прошедшее с момента создания вопроса, до момента создания правильного ответа, превышает 5 минут 
-- или
-- + задавший и ответивший пользователь совпадают и время, прошедшее с момента создания вопроса, до момента создания правильного ответа, превышает 1 час
-- Делается это для того, чтобы отсеять случаи "накрутки" пользователями своей статистики правильных ответов
CREATE TABLE cheat_filtered AS (
  SELECT
    a.owneruserid AS answerer_id,
    unix_timestamp(a.creationdate, "yyyy-MM-dd'T'HH:mm:ss.SSS") - unix_timestamp(q.creationdate, "yyyy-MM-dd'T'HH:mm:ss.SSS") AS seconds_to_answer
  FROM 
    questions q 
    JOIN answers a ON (q.acceptedanswerid = a.id)
  WHERE
    (q.owneruserid <> a.owneruserid 
      AND unix_timestamp(a.creationdate, "yyyy-MM-dd'T'HH:mm:ss.SSS") - unix_timestamp(q.creationdate, "yyyy-MM-dd'T'HH:mm:ss.SSS") > (60 * 5))
  OR (q.owneruserid = a.owneruserid
      AND unix_timestamp(a.creationdate, "yyyy-MM-dd'T'HH:mm:ss.SSS") - unix_timestamp(q.creationdate, "yyyy-MM-dd'T'HH:mm:ss.SSS") > (60 * 60))
);

-- создается временная таблица, в которой содержится id отвечавшего пользователя, среднее время между созданием вопроса и ответом этого пользователя, 
-- и общее число вопросов, на которые правильно ответил пользователь
CREATE temporary TABLE temp AS (
  select 
    answerer_id,
    avg(seconds_to_answer) AS avg_answer_time, 
    count(*) AS total_answers 
  FROM 
    cheat_filtered
  GROUP BY 
    answerer_id 
  HAVING 
    count(*) > 3
);

-- таблица, в которой содержится id отвечающего, его никнейм, среднее время между созданием вопроса и ответом этого пользователя, 
-- и общее число вопросов, на которые правильно ответил пользователь
select 
  t.answerer_id,
  u.displayname,
  t.avg_answer_time,
  t.total_answers
FROM
  temp AS t
  JOIN default.users AS u ON (t.answerer_id = u.id)
ORDER BY 
  t.avg_answer_time;

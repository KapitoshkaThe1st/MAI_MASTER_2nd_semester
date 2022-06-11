package ru.mai.dep806.bigdata.mr;

import java.util.HashMap;
import java.util.Map;

public class SequenceFileUtils {

    public static final char FIELD_SEPARATOR = '\01';
    private static final String FIELD_SEPARATOR_AS_STRING = String.valueOf(FIELD_SEPARATOR);

    public static String toSequenceString(Map<String, String> row, String[] fields) {
        final StringBuilder buffer = new StringBuilder();
        for (String field : fields) {
            String fieldValue = row.get(field);
            if (fieldValue != null) {
                buffer.append(fieldValue);
            }
            buffer.append(FIELD_SEPARATOR);
        }
        return buffer.toString();
    }

    public static Map<String, String> parseFields(String text, String[] fieldNames) {
        String[] parts = text.split(FIELD_SEPARATOR_AS_STRING, -1);

        Map<String, String> result = new HashMap<>();
        for(int i = 0; i < fieldNames.length; ++i){
            result.put(fieldNames[i], parts[i]);
        }

        return result;
    }
}

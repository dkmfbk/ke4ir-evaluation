package eu.fbk.pikesir;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

public enum Field {

    TEXTUAL("textual"),

    URI("uri"),

    TYPE("type"),

    FRAME("frame"),

    TIME("time");

    private static Map<String, Field> idMap = null;

    private final String id;

    private Field(final String id) {
        this.id = id;
    }

    public String getID() {
        return this.id;
    }

    public static Field forID(final String id) {
        if (idMap == null) {
            final ImmutableMap.Builder<String, Field> builder = ImmutableMap.builder();
            for (final Field field : values()) {
                builder.put(field.id, field);
            }
            idMap = builder.build();
        }
        final Field field = idMap.get(id);
        if (field == null) {
            throw new IllegalArgumentException("Unknown field ID '" + id + "'");
        }
        return field;
    }

}
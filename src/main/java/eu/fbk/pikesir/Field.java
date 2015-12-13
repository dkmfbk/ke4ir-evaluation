package eu.fbk.pikesir;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

public enum Field {

    STEM("stem.subword"),

    URI("uri.dbpedia"),

    TYPE_YAGO("type.yago"),

    TYPE_SUMO("type.sumo"),

    PREDICATE_FRB("predicate.frb"),

    PREDICATE_PB("predicate.pb"),

    PREDICATE_NB("predicate.nb"),

    ROLE_FRB("role.frb"),

    ROLE_PB("role.pb"),

    ROLE_NB("role.nb"),

    CONCEPT("concept");

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
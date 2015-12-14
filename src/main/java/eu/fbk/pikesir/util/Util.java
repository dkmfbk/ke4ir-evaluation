package eu.fbk.pikesir.util;

import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.collect.Maps;

public final class Util {

    public static <T> Map<String, T> parseMap(@Nullable final String string,
            final Class<T> valueClass) {
        final Map<String, T> map = Maps.newHashMap();
        if (string != null) {
            for (final String entry : string.split("[\\s;]+")) {
                final int index = Math.max(entry.indexOf(':'), entry.indexOf('='));
                if (index > 0) {
                    final String key = entry.substring(0, index).trim();
                    final String valueString = entry.substring(index + 1);
                    Object value;
                    if (valueClass.isAssignableFrom(Double.class)) {
                        value = Double.valueOf(valueString);
                    } else if (valueClass.isAssignableFrom(Integer.class)) {
                        value = Integer.valueOf(valueString);
                    } else {
                        throw new Error(valueClass.getName());
                    }
                    map.put(key, valueClass.cast(value));
                }
            }
        }
        return map;
    }

    private Util() {
    }

}

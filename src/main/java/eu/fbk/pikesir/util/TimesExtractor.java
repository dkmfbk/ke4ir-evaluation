package eu.fbk.pikesir.util;

import org.openrdf.model.Literal;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.datatype.XMLGregorianCalendar;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by alessio on 17/12/15.
 */

public class TimesExtractor {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimesExtractor.class);

    static Set<String> getTimes(Collection<Literal> literals) {

        HashSet<String> ret = new HashSet<>();

        for (Literal literal : literals) {
            ret.addAll(getTimes(literal));
        }

        return ret;
    }

    static Set<String> getTimes(Literal literal) {
        HashSet<String> ret = new HashSet<>();

        if (!literal.getDatatype().equals(XMLSchema.DATETIME)) {
            XMLGregorianCalendar calendarValue = literal.calendarValue();

            Integer day = calendarValue.getDay();
            Integer month = calendarValue.getMonth();
            Integer year = calendarValue.getYear();
            Integer decade = year / 10;
            Integer century = year / 100;

            if (literal.getDatatype().equals(XMLSchema.DATE)) {
                ret.add("century:" + century);
                ret.add("decade:" + decade);
                ret.add("year:" + year);
                ret.add("month:" + year + "-" + month);
                ret.add("day:" + year + "-" + month + "-" + day);
            }

            if (literal.getDatatype().equals(XMLSchema.GYEARMONTH)) {
                ret.add("century:" + century);
                ret.add("decade:" + decade);
                ret.add("year:" + year);
                ret.add("month:" + year + "-" + month);
            }

            if (literal.getDatatype().equals(XMLSchema.GYEAR)) {
                ret.add("century:" + century);
                ret.add("decade:" + decade);
                ret.add("year:" + year);
            }
        }
        else {
            Integer year = Integer.parseInt(literal.stringValue().substring(0, 4));
            Integer month = Integer.parseInt(literal.stringValue().substring(5, 7));
            Integer day = Integer.parseInt(literal.stringValue().substring(8, 10));
            Integer decade = year / 10;
            Integer century = year / 100;

            ret.add("century:" + century);
            ret.add("decade:" + decade);
            ret.add("year:" + year);
            ret.add("month:" + year + "-" + month);
            ret.add("day:" + year + "-" + month + "-" + day);
        }

        //todo: add to ret

        return ret;
    }

    public static void main(String[] args) {
        LiteralImpl literal;

        literal = new LiteralImpl("2012-10-21", XMLSchema.DATE);
        System.out.println(getTimes(literal));
        literal = new LiteralImpl("2015-02", XMLSchema.GYEARMONTH);
        System.out.println(getTimes(literal));
        literal = new LiteralImpl("2014", XMLSchema.GYEAR);
        System.out.println(getTimes(literal));
        literal = new LiteralImpl("2012-01-20 19:00:00", XMLSchema.DATETIME);
        System.out.println(getTimes(literal));
    }
}

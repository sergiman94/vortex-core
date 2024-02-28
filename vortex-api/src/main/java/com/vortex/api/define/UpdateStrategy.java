
package com.vortex.api.define;

import com.vortex.common.util.E;
import com.vortex.common.util.NumericUtil;
import com.google.common.collect.Sets;

import java.math.BigDecimal;
import java.util.*;

public enum UpdateStrategy {

    // Only number support sum
    SUM {
        @Override
        Object updatePropertyValue(Object oldProperty, Object newProperty) {
            // TODO: Improve preformance? (like write a method in common module)
            BigDecimal oldNumber = new BigDecimal(oldProperty.toString());
            BigDecimal newNumber = new BigDecimal(newProperty.toString());
            return oldNumber.add(newNumber);
        }

        @Override
        void checkPropertyType(Object oldProperty, Object newProperty) {
            E.checkArgument(oldProperty instanceof Number &&
                            newProperty instanceof Number,
                            this.formatError(oldProperty, newProperty,
                                             "Number"));
        }
    },

    // Only Date & Number support compare
    BIGGER {
        @Override
        Object updatePropertyValue(Object oldProperty, Object newProperty) {
            return compareNumber(oldProperty, newProperty, BIGGER);
        }

        @Override
        void checkPropertyType(Object oldProperty, Object newProperty) {
            E.checkArgument((oldProperty instanceof Date ||
                             oldProperty instanceof Number) &&
                            (newProperty instanceof Date ||
                             newProperty instanceof Number),
                            this.formatError(oldProperty, newProperty,
                                             "Date or Number"));
        }
    },

    SMALLER {
        @Override
        Object updatePropertyValue(Object oldProperty, Object newProperty) {
            return compareNumber(oldProperty, newProperty, SMALLER);
        }

        @Override
        void checkPropertyType(Object oldProperty, Object newProperty) {
            E.checkArgument((oldProperty instanceof Date ||
                             oldProperty instanceof Number) &&
                            (newProperty instanceof Date ||
                             newProperty instanceof Number),
                            this.formatError(oldProperty, newProperty,
                                             "Date or Number"));
        }
    },

    // Only Set support union & intersection
    UNION {
        @Override
        Object updatePropertyValue(Object oldProperty, Object newProperty) {
            return combineSet(oldProperty, newProperty, UNION);
        }

        @Override
        void checkPropertyType(Object oldProperty, Object newProperty) {
            // JsonElements are always List-type, so allows two type now.
            this.checkCollectionType(oldProperty, newProperty);
        }
    },

    INTERSECTION {
        @Override
        Object updatePropertyValue(Object oldProperty, Object newProperty) {
            return combineSet(oldProperty, newProperty, INTERSECTION);
        }

        @Override
        void checkPropertyType(Object oldProperty, Object newProperty) {
            this.checkCollectionType(oldProperty, newProperty);
        }
    },

    // Batch update Set should use union because of higher efficiency
    APPEND {
        @Override
        @SuppressWarnings({ "rawtypes", "unchecked" })
        Object updatePropertyValue(Object oldProperty, Object newProperty) {
            ((Collection) oldProperty).addAll((Collection) newProperty);
            return oldProperty;
        }

        @Override
        void checkPropertyType(Object oldProperty, Object newProperty) {
            this.checkCollectionType(oldProperty, newProperty);
        }
    },

    ELIMINATE {
        @Override
        @SuppressWarnings({ "rawtypes", "unchecked" })
        Object updatePropertyValue(Object oldProperty, Object newProperty) {
            ((Collection) oldProperty).removeAll((Collection) newProperty);
            return oldProperty;
        }

        @Override
        void checkPropertyType(Object oldProperty, Object newProperty) {
            this.checkCollectionType(oldProperty, newProperty);
        }
    },

    OVERRIDE {
        @Override
        Object updatePropertyValue(Object oldProperty, Object newProperty) {
            return newProperty;
        }

        @Override
        void checkPropertyType(Object oldProperty, Object newProperty) {
            // Allow any type
        }
    };

    abstract Object updatePropertyValue(Object oldProperty, Object newProperty);

    abstract void checkPropertyType(Object oldProperty, Object newProperty);

    public Object checkAndUpdateProperty(Object oldProperty,
                                         Object newProperty) {
        this.checkPropertyType(oldProperty, newProperty);
        return this.updatePropertyValue(oldProperty, newProperty);
    }

    protected String formatError(Object oldProperty, Object newProperty,
                                 String className) {
        return String.format("Property type must be %s for strategy %s, " +
                             "but got type %s, %s", className, this,
                             oldProperty.getClass().getSimpleName(),
                             newProperty.getClass().getSimpleName());
    }

    protected void checkCollectionType(Object oldProperty,
                                       Object newProperty) {
        E.checkArgument((oldProperty instanceof Set ||
                         oldProperty instanceof List) &&
                        (newProperty instanceof Set ||
                         newProperty instanceof List),
                        this.formatError(oldProperty, newProperty,
                                         "Set or List"));
    }

    protected static Object compareNumber(Object oldProperty,
                                          Object newProperty,
                                          UpdateStrategy strategy) {
        Number oldNum = NumericUtil.convertToNumber(oldProperty);
        Number newNum = NumericUtil.convertToNumber(newProperty);
        int result = NumericUtil.compareNumber(oldNum, newNum);
        return strategy == BIGGER ? (result > 0 ? oldProperty : newProperty) :
                                    (result < 0 ? oldProperty : newProperty);
    }

    protected static Set<?> combineSet(Object oldProperty, Object newProperty,
                                       UpdateStrategy strategy) {
        Set<?> oldSet = oldProperty instanceof Set ?
                        (Set<?>) oldProperty :
                        new HashSet<>((List<?>) oldProperty);
        Set<?> newSet = newProperty instanceof Set ?
                        (Set<?>) newProperty :
                        new HashSet<>((List<?>) newProperty);
        return strategy == UNION ? Sets.union(oldSet, newSet) :
                                   Sets.intersection(oldSet, newSet);
    }
}


package com.vortex.vortexdb.structure;

import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.config.CoreOptions;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.FeatureDescriptor;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.UUID;

public class VortexFeatures implements Graph.Features {

    protected final Vortex graph;
    protected final boolean supportsPersistence;
    protected final VortexGraphFeatures graphFeatures;
    protected final VortexVertexFeatures vertexFeatures;
    protected final VortexEdgeFeatures edgeFeatures;

    public VortexFeatures(Vortex graph, boolean supportsPersistence) {
        this.graph = graph;
        this.supportsPersistence = supportsPersistence;

        this.graphFeatures = new VortexGraphFeatures();
        this.vertexFeatures = new VortexVertexFeatures();
        this.edgeFeatures = new VortexEdgeFeatures();
    }

    @Override
    public VortexGraphFeatures graph() {
        return this.graphFeatures;
    }

    @Override
    public VortexVertexFeatures vertex() {
        return this.vertexFeatures;
    }

    @Override
    public VortexEdgeFeatures edge() {
        return this.edgeFeatures;
    }

    @Override
    public String toString() {
        return StringFactory.featureString(this);
    }

    public class VortexGraphFeatures implements GraphFeatures {

        private final VariableFeatures variableFeatures =
                                       new VortexVariableFeatures();

        @Override
        public boolean supportsConcurrentAccess() {
            return false;
        }

        @Override
        public boolean supportsComputer() {
            return false;
        }

        @Override
        public boolean supportsPersistence() {
            return VortexFeatures.this.supportsPersistence;
        }

        @Override
        public VariableFeatures variables() {
            return this.variableFeatures;
        }

        @Override
        public boolean supportsTransactions() {
            return true;
        }

        @Override
        public boolean supportsThreadedTransactions() {
            return false;
        }
    }

    public class VortexElementFeatures implements ElementFeatures {

        @Override
        public boolean supportsAddProperty() {
            return true;
        }

        @Override
        public boolean supportsRemoveProperty() {
            return true;
        }

        @Override
        public boolean supportsStringIds() {
            return true;
        }

        @Override
        public boolean supportsNumericIds() {
            return false;
        }

        @Override
        public boolean supportsUuidIds() {
            return false;
        }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public boolean supportsCustomIds() {
            return true;
        }

        @Override
        public boolean supportsUserSuppliedIds() {
            return false;
        }

        @Override
        public boolean willAllowId(Object id) {
            if (!this.supportsUserSuppliedIds()) {
                return false;
            } else {
                return this.supportsAnyIds() ||
                       this.supportsCustomIds() && id instanceof Id ||
                       this.supportsStringIds() && id instanceof String ||
                       this.supportsNumericIds() && id instanceof Number ||
                       this.supportsUuidIds() && id instanceof UUID;
            }
        }
    }

    public class VortexVariableFeatures extends VortexDataTypeFeatures
                                      implements VariableFeatures {

    }

    public class VortexVertexPropertyFeatures extends VortexDataTypeFeatures
                                            implements VertexPropertyFeatures {

        @Override
        public boolean supportsRemoveProperty() {
            return true;
        }

        @Override
        public boolean supportsMapValues() {
            return false;
        }

        @Override
        public boolean supportsMixedListValues() {
            return false;
        }

        @Override
        public boolean supportsSerializableValues() {
            return false;
        }

        @Override
        public boolean supportsUniformListValues() {
            return true;
        }

        @Override
        public boolean supportsUserSuppliedIds() {
            return false;
        }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }
    }

    public class VortexEdgePropertyFeatures extends VortexDataTypeFeatures
                                          implements EdgePropertyFeatures {

        @Override
        public boolean supportsMapValues() {
            return false;
        }

        @Override
        public boolean supportsMixedListValues() {
            return false;
        }

        @Override
        public boolean supportsSerializableValues() {
            return false;
        }

        @Override
        public boolean supportsUniformListValues() {
            return true;
        }

    }

    public class VortexVertexFeatures extends VortexElementFeatures
                                    implements VertexFeatures {

        private final VertexPropertyFeatures vertexPropertyFeatures =
                                             new VortexVertexPropertyFeatures();

        @Override
        public boolean supportsUserSuppliedIds() {
            return true;
        }

        @Override
        public VertexPropertyFeatures properties() {
            return this.vertexPropertyFeatures;
        }

        @Override
        public boolean supportsMultiProperties() {
            // Regard as a set (actually can also be a list)
            return true;
        }

        @Override
        public boolean supportsDuplicateMultiProperties() {
            // Regard as a list
            return true;
        }

        @Override
        public boolean supportsMetaProperties() {
            // Nested property
            return false;
        }

        @Override
        public VertexProperty.Cardinality getCardinality(final String key) {
            return VertexProperty.Cardinality.single;
        }

        public boolean supportsDefaultLabel() {
            return true;
        }

        public String defaultLabel() {
            return VortexFeatures.this.graph
                               .option(CoreOptions.VERTEX_DEFAULT_LABEL);
        }
    }

    public class VortexEdgeFeatures extends VortexElementFeatures
                                  implements EdgeFeatures {

        private final EdgePropertyFeatures edgePropertyFeatures =
                                           new VortexEdgePropertyFeatures();

        @Override
        public EdgePropertyFeatures properties() {
            return this.edgePropertyFeatures;
        }
    }

    public class VortexDataTypeFeatures implements DataTypeFeatures {

        @Override
        @FeatureDescriptor(name = FEATURE_STRING_VALUES)
        public boolean supportsStringValues() {
            return true;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_BOOLEAN_VALUES)
        public boolean supportsBooleanValues() {
            return true;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_BYTE_VALUES)
        public boolean supportsByteValues() {
            return true;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_FLOAT_VALUES)
        public boolean supportsFloatValues() {
            return true;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_DOUBLE_VALUES)
        public boolean supportsDoubleValues() {
            return true;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_INTEGER_VALUES)
        public boolean supportsIntegerValues() {
            return true;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_LONG_VALUES)
        public boolean supportsLongValues() {
            return true;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_UNIFORM_LIST_VALUES)
        public boolean supportsUniformListValues() {
            /*
             * NOTE: must use cardinality list if use LIST property value,
             * can't support a LIST property value with cardinality single
             */
            return true;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_MIXED_LIST_VALUES)
        public boolean supportsMixedListValues() {
            return false;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_MAP_VALUES)
        public boolean supportsMapValues() {
            return false;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_SERIALIZABLE_VALUES)
        public boolean supportsSerializableValues() {
            return false;
        }

        /**
         * All these supportsXxArrayValues() must be used with cardinality list
         * we can't support array values with cardinality single like tinkerpop
         */
        @Override
        @FeatureDescriptor(name = FEATURE_BYTE_ARRAY_VALUES)
        public boolean supportsByteArrayValues() {
            // Regard as blob
            return true;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_BOOLEAN_ARRAY_VALUES)
        public boolean supportsBooleanArrayValues() {
            return false;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_FLOAT_ARRAY_VALUES)
        public boolean supportsFloatArrayValues() {
            return false;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_DOUBLE_ARRAY_VALUES)
        public boolean supportsDoubleArrayValues() {
            return false;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_INTEGER_ARRAY_VALUES)
        public boolean supportsIntegerArrayValues() {
            return false;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_LONG_ARRAY_VALUES)
        public boolean supportsLongArrayValues() {
            return false;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_STRING_ARRAY_VALUES)
        public boolean supportsStringArrayValues() {
            return false;
        }
    }
}


package com.vortex.vortexdb.backend.store.raft.rpc;

@SuppressWarnings("unused")
public final class RaftRequests {
  private RaftRequests() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  /**
   * Protobuf enum {@code com.vortex.vortexdb.backend.store.raft.rpc.StoreType}
   */
  public enum StoreType
      implements com.google.protobuf.ProtocolMessageEnum {
    /**
     * <code>SCHEMA = 0;</code>
     */
    SCHEMA(0, 0),
    /**
     * <code>GRAPH = 1;</code>
     */
    GRAPH(1, 1),
    /**
     * <code>SYSTEM = 2;</code>
     */
    SYSTEM(2, 2),
    /**
     * <code>ALL = 3;</code>
     */
    ALL(3, 3),
    ;

    /**
     * <code>SCHEMA = 0;</code>
     */
    public static final int SCHEMA_VALUE = 0;
    /**
     * <code>GRAPH = 1;</code>
     */
    public static final int GRAPH_VALUE = 1;
    /**
     * <code>SYSTEM = 2;</code>
     */
    public static final int SYSTEM_VALUE = 2;
    /**
     * <code>ALL = 3;</code>
     */
    public static final int ALL_VALUE = 3;


    public final int getNumber() { return value; }

    public static StoreType valueOf(int value) {
      switch (value) {
        case 0: return SCHEMA;
        case 1: return GRAPH;
        case 2: return SYSTEM;
        case 3: return ALL;
        default: return null;
      }
    }

    public static com.google.protobuf.Internal.EnumLiteMap<StoreType>
        internalGetValueMap() {
      return internalValueMap;
    }
    private static com.google.protobuf.Internal.EnumLiteMap<StoreType>
        internalValueMap =
          new com.google.protobuf.Internal.EnumLiteMap<StoreType>() {
            public StoreType findValueByNumber(int number) {
              return StoreType.valueOf(number);
            }
          };

    public final com.google.protobuf.Descriptors.EnumValueDescriptor
        getValueDescriptor() {
      return getDescriptor().getValues().get(index);
    }
    public final com.google.protobuf.Descriptors.EnumDescriptor
        getDescriptorForType() {
      return getDescriptor();
    }
    public static final com.google.protobuf.Descriptors.EnumDescriptor
        getDescriptor() {
      return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.getDescriptor().getEnumTypes().get(0);
    }

    private static final StoreType[] VALUES = values();

    public static StoreType valueOf(
        com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
      if (desc.getType() != getDescriptor()) {
        throw new IllegalArgumentException(
          "EnumValueDescriptor is not for this type.");
      }
      return VALUES[desc.getIndex()];
    }

    private final int index;
    private final int value;

    private StoreType(int index, int value) {
      this.index = index;
      this.value = value;
    }

    // @@protoc_insertion_point(enum_scope:com.vortex.vortexdb.backend.store.raft.rpc.StoreType)
  }

  /**
   * Protobuf enum {@code com.vortex.vortexdb.backend.store.raft.rpc.StoreAction}
   */
  public enum StoreAction
      implements com.google.protobuf.ProtocolMessageEnum {
    /**
     * <code>NONE = 0;</code>
     */
    NONE(0, 0),
    /**
     * <code>INIT = 1;</code>
     */
    INIT(1, 1),
    /**
     * <code>CLEAR = 2;</code>
     */
    CLEAR(2, 2),
    /**
     * <code>TRUNCATE = 3;</code>
     */
    TRUNCATE(3, 3),
    /**
     * <code>SNAPSHOT = 4;</code>
     */
    SNAPSHOT(4, 4),
    /**
     * <code>BEGIN_TX = 10;</code>
     */
    BEGIN_TX(5, 10),
    /**
     * <code>COMMIT_TX = 11;</code>
     */
    COMMIT_TX(6, 11),
    /**
     * <code>ROLLBACK_TX = 12;</code>
     */
    ROLLBACK_TX(7, 12),
    /**
     * <code>MUTATE = 20;</code>
     */
    MUTATE(8, 20),
    /**
     * <code>INCR_COUNTER = 21;</code>
     */
    INCR_COUNTER(9, 21),
    /**
     * <code>QUERY = 30;</code>
     */
    QUERY(10, 30),
    ;

    /**
     * <code>NONE = 0;</code>
     */
    public static final int NONE_VALUE = 0;
    /**
     * <code>INIT = 1;</code>
     */
    public static final int INIT_VALUE = 1;
    /**
     * <code>CLEAR = 2;</code>
     */
    public static final int CLEAR_VALUE = 2;
    /**
     * <code>TRUNCATE = 3;</code>
     */
    public static final int TRUNCATE_VALUE = 3;
    /**
     * <code>SNAPSHOT = 4;</code>
     */
    public static final int SNAPSHOT_VALUE = 4;
    /**
     * <code>BEGIN_TX = 10;</code>
     */
    public static final int BEGIN_TX_VALUE = 10;
    /**
     * <code>COMMIT_TX = 11;</code>
     */
    public static final int COMMIT_TX_VALUE = 11;
    /**
     * <code>ROLLBACK_TX = 12;</code>
     */
    public static final int ROLLBACK_TX_VALUE = 12;
    /**
     * <code>MUTATE = 20;</code>
     */
    public static final int MUTATE_VALUE = 20;
    /**
     * <code>INCR_COUNTER = 21;</code>
     */
    public static final int INCR_COUNTER_VALUE = 21;
    /**
     * <code>QUERY = 30;</code>
     */
    public static final int QUERY_VALUE = 30;


    public final int getNumber() { return value; }

    public static StoreAction valueOf(int value) {
      switch (value) {
        case 0: return NONE;
        case 1: return INIT;
        case 2: return CLEAR;
        case 3: return TRUNCATE;
        case 4: return SNAPSHOT;
        case 10: return BEGIN_TX;
        case 11: return COMMIT_TX;
        case 12: return ROLLBACK_TX;
        case 20: return MUTATE;
        case 21: return INCR_COUNTER;
        case 30: return QUERY;
        default: return null;
      }
    }

    public static com.google.protobuf.Internal.EnumLiteMap<StoreAction>
        internalGetValueMap() {
      return internalValueMap;
    }
    private static com.google.protobuf.Internal.EnumLiteMap<StoreAction>
        internalValueMap =
          new com.google.protobuf.Internal.EnumLiteMap<StoreAction>() {
            public StoreAction findValueByNumber(int number) {
              return StoreAction.valueOf(number);
            }
          };

    public final com.google.protobuf.Descriptors.EnumValueDescriptor
        getValueDescriptor() {
      return getDescriptor().getValues().get(index);
    }
    public final com.google.protobuf.Descriptors.EnumDescriptor
        getDescriptorForType() {
      return getDescriptor();
    }
    public static final com.google.protobuf.Descriptors.EnumDescriptor
        getDescriptor() {
      return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.getDescriptor().getEnumTypes().get(1);
    }

    private static final StoreAction[] VALUES = values();

    public static StoreAction valueOf(
        com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
      if (desc.getType() != getDescriptor()) {
        throw new IllegalArgumentException(
          "EnumValueDescriptor is not for this type.");
      }
      return VALUES[desc.getIndex()];
    }

    private final int index;
    private final int value;

    private StoreAction(int index, int value) {
      this.index = index;
      this.value = value;
    }

    // @@protoc_insertion_point(enum_scope:com.vortex.vortexdb.backend.store.raft.rpc.StoreAction)
  }

  public interface StoreCommandRequestOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // required .com.vortex.vortexdb.backend.store.raft.rpc.StoreType type = 1;
    /**
     * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.StoreType type = 1;</code>
     */
    boolean hasType();
    /**
     * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.StoreType type = 1;</code>
     */
    com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreType getType();

    // required .com.vortex.vortexdb.backend.store.raft.rpc.StoreAction action = 2;
    /**
     * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.StoreAction action = 2;</code>
     */
    boolean hasAction();
    /**
     * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.StoreAction action = 2;</code>
     */
    com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreAction getAction();

    // required bytes data = 3;
    /**
     * <code>required bytes data = 3;</code>
     */
    boolean hasData();
    /**
     * <code>required bytes data = 3;</code>
     */
    com.google.protobuf.ByteString getData();
  }
  /**
   * Protobuf type {@code com.vortex.vortexdb.backend.store.raft.rpc.StoreCommandRequest}
   */
  public static final class StoreCommandRequest extends
      com.google.protobuf.GeneratedMessage
      implements StoreCommandRequestOrBuilder {
    // Use StoreCommandRequest.newBuilder() to construct.
    private StoreCommandRequest(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private StoreCommandRequest(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final StoreCommandRequest defaultInstance;
    public static StoreCommandRequest getDefaultInstance() {
      return defaultInstance;
    }

    public StoreCommandRequest getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private StoreCommandRequest(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 8: {
              int rawValue = input.readEnum();
              com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreType value = com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreType.valueOf(rawValue);
              if (value == null) {
                unknownFields.mergeVarintField(1, rawValue);
              } else {
                bitField0_ |= 0x00000001;
                type_ = value;
              }
              break;
            }
            case 16: {
              int rawValue = input.readEnum();
              com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreAction value = com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreAction.valueOf(rawValue);
              if (value == null) {
                unknownFields.mergeVarintField(2, rawValue);
              } else {
                bitField0_ |= 0x00000002;
                action_ = value;
              }
              break;
            }
            case 26: {
              bitField0_ |= 0x00000004;
              data_ = input.readBytes();
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.internal_static_com_vortex_vortexdb_backend_store_raft_rpc_StoreCommandRequest_descriptor;
    }

    protected FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.internal_static_com_vortex_vortexdb_backend_store_raft_rpc_StoreCommandRequest_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandRequest.class, com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandRequest.Builder.class);
    }

    public static com.google.protobuf.Parser<StoreCommandRequest> PARSER =
        new com.google.protobuf.AbstractParser<StoreCommandRequest>() {
      public StoreCommandRequest parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new StoreCommandRequest(input, extensionRegistry);
      }
    };

    @Override
    public com.google.protobuf.Parser<StoreCommandRequest> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    // required .com.vortex.vortexdb.backend.store.raft.rpc.StoreType type = 1;
    public static final int TYPE_FIELD_NUMBER = 1;
    private com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreType type_;
    /**
     * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.StoreType type = 1;</code>
     */
    public boolean hasType() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.StoreType type = 1;</code>
     */
    public com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreType getType() {
      return type_;
    }

    // required .com.vortex.vortexdb.backend.store.raft.rpc.StoreAction action = 2;
    public static final int ACTION_FIELD_NUMBER = 2;
    private com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreAction action_;
    /**
     * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.StoreAction action = 2;</code>
     */
    public boolean hasAction() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.StoreAction action = 2;</code>
     */
    public com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreAction getAction() {
      return action_;
    }

    // required bytes data = 3;
    public static final int DATA_FIELD_NUMBER = 3;
    private com.google.protobuf.ByteString data_;
    /**
     * <code>required bytes data = 3;</code>
     */
    public boolean hasData() {
      return ((bitField0_ & 0x00000004) == 0x00000004);
    }
    /**
     * <code>required bytes data = 3;</code>
     */
    public com.google.protobuf.ByteString getData() {
      return data_;
    }

    private void initFields() {
      type_ = com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreType.SCHEMA;
      action_ = com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreAction.NONE;
      data_ = com.google.protobuf.ByteString.EMPTY;
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      if (!hasType()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!hasAction()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!hasData()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeEnum(1, type_.getNumber());
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        output.writeEnum(2, action_.getNumber());
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        output.writeBytes(3, data_);
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeEnumSize(1, type_.getNumber());
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        size += com.google.protobuf.CodedOutputStream
          .computeEnumSize(2, action_.getNumber());
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(3, data_);
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @Override
    protected Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandRequest parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandRequest parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandRequest parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandRequest parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandRequest parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandRequest parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandRequest parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandRequest parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandRequest parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandRequest parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandRequest prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @Override
    protected Builder newBuilderForType(
        BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code com.vortex.vortexdb.backend.store.raft.rpc.StoreCommandRequest}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandRequestOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.internal_static_com_vortex_vortexdb_backend_store_raft_rpc_StoreCommandRequest_descriptor;
      }

      protected FieldAccessorTable
          internalGetFieldAccessorTable() {
        return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.internal_static_com_vortex_vortexdb_backend_store_raft_rpc_StoreCommandRequest_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandRequest.class, com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandRequest.Builder.class);
      }

      // Construct using com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandRequest.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        type_ = com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreType.SCHEMA;
        bitField0_ = (bitField0_ & ~0x00000001);
        action_ = com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreAction.NONE;
        bitField0_ = (bitField0_ & ~0x00000002);
        data_ = com.google.protobuf.ByteString.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000004);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.internal_static_com_vortex_vortexdb_backend_store_raft_rpc_StoreCommandRequest_descriptor;
      }

      public com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandRequest getDefaultInstanceForType() {
        return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandRequest.getDefaultInstance();
      }

      public com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandRequest build() {
        com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandRequest result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandRequest buildPartial() {
        com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandRequest result = new com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandRequest(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.type_ = type_;
        if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
          to_bitField0_ |= 0x00000002;
        }
        result.action_ = action_;
        if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
          to_bitField0_ |= 0x00000004;
        }
        result.data_ = data_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandRequest) {
          return mergeFrom((com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandRequest)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandRequest other) {
        if (other == com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandRequest.getDefaultInstance()) return this;
        if (other.hasType()) {
          setType(other.getType());
        }
        if (other.hasAction()) {
          setAction(other.getAction());
        }
        if (other.hasData()) {
          setData(other.getData());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        if (!hasType()) {

          return false;
        }
        if (!hasAction()) {

          return false;
        }
        if (!hasData()) {

          return false;
        }
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandRequest parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandRequest) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // required .com.vortex.vortexdb.backend.store.raft.rpc.StoreType type = 1;
      private com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreType type_ = com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreType.SCHEMA;
      /**
       * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.StoreType type = 1;</code>
       */
      public boolean hasType() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.StoreType type = 1;</code>
       */
      public com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreType getType() {
        return type_;
      }
      /**
       * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.StoreType type = 1;</code>
       */
      public Builder setType(com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreType value) {
        if (value == null) {
          throw new NullPointerException();
        }
        bitField0_ |= 0x00000001;
        type_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.StoreType type = 1;</code>
       */
      public Builder clearType() {
        bitField0_ = (bitField0_ & ~0x00000001);
        type_ = com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreType.SCHEMA;
        onChanged();
        return this;
      }

      // required .com.vortex.vortexdb.backend.store.raft.rpc.StoreAction action = 2;
      private com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreAction action_ = com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreAction.NONE;
      /**
       * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.StoreAction action = 2;</code>
       */
      public boolean hasAction() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      /**
       * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.StoreAction action = 2;</code>
       */
      public com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreAction getAction() {
        return action_;
      }
      /**
       * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.StoreAction action = 2;</code>
       */
      public Builder setAction(com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreAction value) {
        if (value == null) {
          throw new NullPointerException();
        }
        bitField0_ |= 0x00000002;
        action_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.StoreAction action = 2;</code>
       */
      public Builder clearAction() {
        bitField0_ = (bitField0_ & ~0x00000002);
        action_ = com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreAction.NONE;
        onChanged();
        return this;
      }

      // required bytes data = 3;
      private com.google.protobuf.ByteString data_ = com.google.protobuf.ByteString.EMPTY;
      /**
       * <code>required bytes data = 3;</code>
       */
      public boolean hasData() {
        return ((bitField0_ & 0x00000004) == 0x00000004);
      }
      /**
       * <code>required bytes data = 3;</code>
       */
      public com.google.protobuf.ByteString getData() {
        return data_;
      }
      /**
       * <code>required bytes data = 3;</code>
       */
      public Builder setData(com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000004;
        data_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required bytes data = 3;</code>
       */
      public Builder clearData() {
        bitField0_ = (bitField0_ & ~0x00000004);
        data_ = getDefaultInstance().getData();
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:com.vortex.vortexdb.backend.store.raft.rpc.StoreCommandRequest)
    }

    static {
      defaultInstance = new StoreCommandRequest(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:com.vortex.vortexdb.backend.store.raft.rpc.StoreCommandRequest)
  }

  public interface StoreCommandResponseOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // required bool status = 1;
    /**
     * <code>required bool status = 1;</code>
     */
    boolean hasStatus();
    /**
     * <code>required bool status = 1;</code>
     */
    boolean getStatus();

    // optional string message = 2;
    /**
     * <code>optional string message = 2;</code>
     */
    boolean hasMessage();
    /**
     * <code>optional string message = 2;</code>
     */
    String getMessage();
    /**
     * <code>optional string message = 2;</code>
     */
    com.google.protobuf.ByteString
        getMessageBytes();
  }
  /**
   * Protobuf type {@code com.vortex.vortexdb.backend.store.raft.rpc.StoreCommandResponse}
   */
  public static final class StoreCommandResponse extends
      com.google.protobuf.GeneratedMessage
      implements StoreCommandResponseOrBuilder {
    // Use StoreCommandResponse.newBuilder() to construct.
    private StoreCommandResponse(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private StoreCommandResponse(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final StoreCommandResponse defaultInstance;
    public static StoreCommandResponse getDefaultInstance() {
      return defaultInstance;
    }

    public StoreCommandResponse getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private StoreCommandResponse(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 8: {
              bitField0_ |= 0x00000001;
              status_ = input.readBool();
              break;
            }
            case 18: {
              bitField0_ |= 0x00000002;
              message_ = input.readBytes();
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.internal_static_com_vortex_vortexdb_backend_store_raft_rpc_StoreCommandResponse_descriptor;
    }

    protected FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.internal_static_com_vortex_vortexdb_backend_store_raft_rpc_StoreCommandResponse_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandResponse.class, com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandResponse.Builder.class);
    }

    public static com.google.protobuf.Parser<StoreCommandResponse> PARSER =
        new com.google.protobuf.AbstractParser<StoreCommandResponse>() {
      public StoreCommandResponse parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new StoreCommandResponse(input, extensionRegistry);
      }
    };

    @Override
    public com.google.protobuf.Parser<StoreCommandResponse> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    // required bool status = 1;
    public static final int STATUS_FIELD_NUMBER = 1;
    private boolean status_;
    /**
     * <code>required bool status = 1;</code>
     */
    public boolean hasStatus() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>required bool status = 1;</code>
     */
    public boolean getStatus() {
      return status_;
    }

    // optional string message = 2;
    public static final int MESSAGE_FIELD_NUMBER = 2;
    private Object message_;
    /**
     * <code>optional string message = 2;</code>
     */
    public boolean hasMessage() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional string message = 2;</code>
     */
    public String getMessage() {
      Object ref = message_;
      if (ref instanceof String) {
        return (String) ref;
      } else {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          message_ = s;
        }
        return s;
      }
    }
    /**
     * <code>optional string message = 2;</code>
     */
    public com.google.protobuf.ByteString
        getMessageBytes() {
      Object ref = message_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b =
            com.google.protobuf.ByteString.copyFromUtf8(
                (String) ref);
        message_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    private void initFields() {
      status_ = false;
      message_ = "";
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      if (!hasStatus()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeBool(1, status_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        output.writeBytes(2, getMessageBytes());
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBoolSize(1, status_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(2, getMessageBytes());
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @Override
    protected Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandResponse parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandResponse parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandResponse parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandResponse parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandResponse parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandResponse parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandResponse parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandResponse parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandResponse parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandResponse parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandResponse prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @Override
    protected Builder newBuilderForType(
        BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code com.vortex.vortexdb.backend.store.raft.rpc.StoreCommandResponse}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandResponseOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.internal_static_com_vortex_vortexdb_backend_store_raft_rpc_StoreCommandResponse_descriptor;
      }

      protected FieldAccessorTable
          internalGetFieldAccessorTable() {
        return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.internal_static_com_vortex_vortexdb_backend_store_raft_rpc_StoreCommandResponse_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandResponse.class, com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandResponse.Builder.class);
      }

      // Construct using com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandResponse.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        status_ = false;
        bitField0_ = (bitField0_ & ~0x00000001);
        message_ = "";
        bitField0_ = (bitField0_ & ~0x00000002);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.internal_static_com_vortex_vortexdb_backend_store_raft_rpc_StoreCommandResponse_descriptor;
      }

      public com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandResponse getDefaultInstanceForType() {
        return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandResponse.getDefaultInstance();
      }

      public com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandResponse build() {
        com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandResponse result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandResponse buildPartial() {
        com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandResponse result = new com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandResponse(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.status_ = status_;
        if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
          to_bitField0_ |= 0x00000002;
        }
        result.message_ = message_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandResponse) {
          return mergeFrom((com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandResponse)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandResponse other) {
        if (other == com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandResponse.getDefaultInstance()) return this;
        if (other.hasStatus()) {
          setStatus(other.getStatus());
        }
        if (other.hasMessage()) {
          bitField0_ |= 0x00000002;
          message_ = other.message_;
          onChanged();
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        if (!hasStatus()) {

          return false;
        }
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandResponse parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.StoreCommandResponse) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // required bool status = 1;
      private boolean status_ ;
      /**
       * <code>required bool status = 1;</code>
       */
      public boolean hasStatus() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>required bool status = 1;</code>
       */
      public boolean getStatus() {
        return status_;
      }
      /**
       * <code>required bool status = 1;</code>
       */
      public Builder setStatus(boolean value) {
        bitField0_ |= 0x00000001;
        status_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required bool status = 1;</code>
       */
      public Builder clearStatus() {
        bitField0_ = (bitField0_ & ~0x00000001);
        status_ = false;
        onChanged();
        return this;
      }

      // optional string message = 2;
      private Object message_ = "";
      /**
       * <code>optional string message = 2;</code>
       */
      public boolean hasMessage() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      /**
       * <code>optional string message = 2;</code>
       */
      public String getMessage() {
        Object ref = message_;
        if (!(ref instanceof String)) {
          String s = ((com.google.protobuf.ByteString) ref)
              .toStringUtf8();
          message_ = s;
          return s;
        } else {
          return (String) ref;
        }
      }
      /**
       * <code>optional string message = 2;</code>
       */
      public com.google.protobuf.ByteString
          getMessageBytes() {
        Object ref = message_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b =
              com.google.protobuf.ByteString.copyFromUtf8(
                  (String) ref);
          message_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>optional string message = 2;</code>
       */
      public Builder setMessage(
          String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        message_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional string message = 2;</code>
       */
      public Builder clearMessage() {
        bitField0_ = (bitField0_ & ~0x00000002);
        message_ = getDefaultInstance().getMessage();
        onChanged();
        return this;
      }
      /**
       * <code>optional string message = 2;</code>
       */
      public Builder setMessageBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        message_ = value;
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:com.vortex.vortexdb.backend.store.raft.rpc.StoreCommandResponse)
    }

    static {
      defaultInstance = new StoreCommandResponse(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:com.vortex.vortexdb.backend.store.raft.rpc.StoreCommandResponse)
  }

  public interface CommonResponseOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // required bool status = 1;
    /**
     * <code>required bool status = 1;</code>
     */
    boolean hasStatus();
    /**
     * <code>required bool status = 1;</code>
     */
    boolean getStatus();

    // optional string message = 2;
    /**
     * <code>optional string message = 2;</code>
     */
    boolean hasMessage();
    /**
     * <code>optional string message = 2;</code>
     */
    String getMessage();
    /**
     * <code>optional string message = 2;</code>
     */
    com.google.protobuf.ByteString
        getMessageBytes();
  }
  /**
   * Protobuf type {@code com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse}
   */
  public static final class CommonResponse extends
      com.google.protobuf.GeneratedMessage
      implements CommonResponseOrBuilder {
    // Use CommonResponse.newBuilder() to construct.
    private CommonResponse(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private CommonResponse(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final CommonResponse defaultInstance;
    public static CommonResponse getDefaultInstance() {
      return defaultInstance;
    }

    public CommonResponse getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private CommonResponse(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 8: {
              bitField0_ |= 0x00000001;
              status_ = input.readBool();
              break;
            }
            case 18: {
              bitField0_ |= 0x00000002;
              message_ = input.readBytes();
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.internal_static_com_vortex_vortexdb_backend_store_raft_rpc_CommonResponse_descriptor;
    }

    protected FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.internal_static_com_vortex_vortexdb_backend_store_raft_rpc_CommonResponse_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse.class, com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse.Builder.class);
    }

    public static com.google.protobuf.Parser<CommonResponse> PARSER =
        new com.google.protobuf.AbstractParser<CommonResponse>() {
      public CommonResponse parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new CommonResponse(input, extensionRegistry);
      }
    };

    @Override
    public com.google.protobuf.Parser<CommonResponse> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    // required bool status = 1;
    public static final int STATUS_FIELD_NUMBER = 1;
    private boolean status_;
    /**
     * <code>required bool status = 1;</code>
     */
    public boolean hasStatus() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>required bool status = 1;</code>
     */
    public boolean getStatus() {
      return status_;
    }

    // optional string message = 2;
    public static final int MESSAGE_FIELD_NUMBER = 2;
    private Object message_;
    /**
     * <code>optional string message = 2;</code>
     */
    public boolean hasMessage() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional string message = 2;</code>
     */
    public String getMessage() {
      Object ref = message_;
      if (ref instanceof String) {
        return (String) ref;
      } else {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          message_ = s;
        }
        return s;
      }
    }
    /**
     * <code>optional string message = 2;</code>
     */
    public com.google.protobuf.ByteString
        getMessageBytes() {
      Object ref = message_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b =
            com.google.protobuf.ByteString.copyFromUtf8(
                (String) ref);
        message_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    private void initFields() {
      status_ = false;
      message_ = "";
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      if (!hasStatus()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeBool(1, status_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        output.writeBytes(2, getMessageBytes());
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBoolSize(1, status_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(2, getMessageBytes());
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @Override
    protected Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @Override
    protected Builder newBuilderForType(
        BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponseOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.internal_static_com_vortex_vortexdb_backend_store_raft_rpc_CommonResponse_descriptor;
      }

      protected FieldAccessorTable
          internalGetFieldAccessorTable() {
        return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.internal_static_com_vortex_vortexdb_backend_store_raft_rpc_CommonResponse_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse.class, com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse.Builder.class);
      }

      // Construct using com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        status_ = false;
        bitField0_ = (bitField0_ & ~0x00000001);
        message_ = "";
        bitField0_ = (bitField0_ & ~0x00000002);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.internal_static_com_vortex_vortexdb_backend_store_raft_rpc_CommonResponse_descriptor;
      }

      public com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse getDefaultInstanceForType() {
        return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse.getDefaultInstance();
      }

      public com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse build() {
        com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse buildPartial() {
        com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse result = new com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.status_ = status_;
        if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
          to_bitField0_ |= 0x00000002;
        }
        result.message_ = message_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse) {
          return mergeFrom((com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse other) {
        if (other == com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse.getDefaultInstance()) return this;
        if (other.hasStatus()) {
          setStatus(other.getStatus());
        }
        if (other.hasMessage()) {
          bitField0_ |= 0x00000002;
          message_ = other.message_;
          onChanged();
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        if (!hasStatus()) {

          return false;
        }
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // required bool status = 1;
      private boolean status_ ;
      /**
       * <code>required bool status = 1;</code>
       */
      public boolean hasStatus() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>required bool status = 1;</code>
       */
      public boolean getStatus() {
        return status_;
      }
      /**
       * <code>required bool status = 1;</code>
       */
      public Builder setStatus(boolean value) {
        bitField0_ |= 0x00000001;
        status_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required bool status = 1;</code>
       */
      public Builder clearStatus() {
        bitField0_ = (bitField0_ & ~0x00000001);
        status_ = false;
        onChanged();
        return this;
      }

      // optional string message = 2;
      private Object message_ = "";
      /**
       * <code>optional string message = 2;</code>
       */
      public boolean hasMessage() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      /**
       * <code>optional string message = 2;</code>
       */
      public String getMessage() {
        Object ref = message_;
        if (!(ref instanceof String)) {
          String s = ((com.google.protobuf.ByteString) ref)
              .toStringUtf8();
          message_ = s;
          return s;
        } else {
          return (String) ref;
        }
      }
      /**
       * <code>optional string message = 2;</code>
       */
      public com.google.protobuf.ByteString
          getMessageBytes() {
        Object ref = message_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b =
              com.google.protobuf.ByteString.copyFromUtf8(
                  (String) ref);
          message_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>optional string message = 2;</code>
       */
      public Builder setMessage(
          String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        message_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional string message = 2;</code>
       */
      public Builder clearMessage() {
        bitField0_ = (bitField0_ & ~0x00000002);
        message_ = getDefaultInstance().getMessage();
        onChanged();
        return this;
      }
      /**
       * <code>optional string message = 2;</code>
       */
      public Builder setMessageBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        message_ = value;
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse)
    }

    static {
      defaultInstance = new CommonResponse(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse)
  }

  public interface ListPeersRequestOrBuilder
      extends com.google.protobuf.MessageOrBuilder {
  }
  /**
   * Protobuf type {@code com.vortex.vortexdb.backend.store.raft.rpc.ListPeersRequest}
   */
  public static final class ListPeersRequest extends
      com.google.protobuf.GeneratedMessage
      implements ListPeersRequestOrBuilder {
    // Use ListPeersRequest.newBuilder() to construct.
    private ListPeersRequest(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private ListPeersRequest(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final ListPeersRequest defaultInstance;
    public static ListPeersRequest getDefaultInstance() {
      return defaultInstance;
    }

    public ListPeersRequest getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private ListPeersRequest(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.internal_static_com_vortex_vortexdb_backend_store_raft_rpc_ListPeersRequest_descriptor;
    }

    protected FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.internal_static_com_vortex_vortexdb_backend_store_raft_rpc_ListPeersRequest_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersRequest.class, com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersRequest.Builder.class);
    }

    public static com.google.protobuf.Parser<ListPeersRequest> PARSER =
        new com.google.protobuf.AbstractParser<ListPeersRequest>() {
      public ListPeersRequest parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new ListPeersRequest(input, extensionRegistry);
      }
    };

    @Override
    public com.google.protobuf.Parser<ListPeersRequest> getParserForType() {
      return PARSER;
    }

    private void initFields() {
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @Override
    protected Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersRequest parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersRequest parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersRequest parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersRequest parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersRequest parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersRequest parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersRequest parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersRequest parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersRequest parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersRequest parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersRequest prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @Override
    protected Builder newBuilderForType(
        BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code com.vortex.vortexdb.backend.store.raft.rpc.ListPeersRequest}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersRequestOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.internal_static_com_vortex_vortexdb_backend_store_raft_rpc_ListPeersRequest_descriptor;
      }

      protected FieldAccessorTable
          internalGetFieldAccessorTable() {
        return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.internal_static_com_vortex_vortexdb_backend_store_raft_rpc_ListPeersRequest_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersRequest.class, com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersRequest.Builder.class);
      }

      // Construct using com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersRequest.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.internal_static_com_vortex_vortexdb_backend_store_raft_rpc_ListPeersRequest_descriptor;
      }

      public com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersRequest getDefaultInstanceForType() {
        return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersRequest.getDefaultInstance();
      }

      public com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersRequest build() {
        com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersRequest result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersRequest buildPartial() {
        com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersRequest result = new com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersRequest(this);
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersRequest) {
          return mergeFrom((com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersRequest)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersRequest other) {
        if (other == com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersRequest.getDefaultInstance()) return this;
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersRequest parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersRequest) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }

      // @@protoc_insertion_point(builder_scope:com.vortex.vortexdb.backend.store.raft.rpc.ListPeersRequest)
    }

    static {
      defaultInstance = new ListPeersRequest(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:com.vortex.vortexdb.backend.store.raft.rpc.ListPeersRequest)
  }

  public interface ListPeersResponseOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // required .com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse common = 1;
    /**
     * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse common = 1;</code>
     */
    boolean hasCommon();
    /**
     * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse common = 1;</code>
     */
    com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse getCommon();
    /**
     * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse common = 1;</code>
     */
    com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponseOrBuilder getCommonOrBuilder();

    // repeated string endpoints = 2;
    /**
     * <code>repeated string endpoints = 2;</code>
     */
    java.util.List<String>
    getEndpointsList();
    /**
     * <code>repeated string endpoints = 2;</code>
     */
    int getEndpointsCount();
    /**
     * <code>repeated string endpoints = 2;</code>
     */
    String getEndpoints(int index);
    /**
     * <code>repeated string endpoints = 2;</code>
     */
    com.google.protobuf.ByteString
        getEndpointsBytes(int index);
  }
  /**
   * Protobuf type {@code com.vortex.vortexdb.backend.store.raft.rpc.ListPeersResponse}
   */
  public static final class ListPeersResponse extends
      com.google.protobuf.GeneratedMessage
      implements ListPeersResponseOrBuilder {
    // Use ListPeersResponse.newBuilder() to construct.
    private ListPeersResponse(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private ListPeersResponse(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final ListPeersResponse defaultInstance;
    public static ListPeersResponse getDefaultInstance() {
      return defaultInstance;
    }

    public ListPeersResponse getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private ListPeersResponse(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 10: {
              com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse.Builder subBuilder = null;
              if (((bitField0_ & 0x00000001) == 0x00000001)) {
                subBuilder = common_.toBuilder();
              }
              common_ = input.readMessage(com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse.PARSER, extensionRegistry);
              if (subBuilder != null) {
                subBuilder.mergeFrom(common_);
                common_ = subBuilder.buildPartial();
              }
              bitField0_ |= 0x00000001;
              break;
            }
            case 18: {
              if (!((mutable_bitField0_ & 0x00000002) == 0x00000002)) {
                endpoints_ = new com.google.protobuf.LazyStringArrayList();
                mutable_bitField0_ |= 0x00000002;
              }
              endpoints_.add(input.readBytes());
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        if (((mutable_bitField0_ & 0x00000002) == 0x00000002)) {
          endpoints_ = new com.google.protobuf.UnmodifiableLazyStringList(endpoints_);
        }
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.internal_static_com_vortex_vortexdb_backend_store_raft_rpc_ListPeersResponse_descriptor;
    }

    protected FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.internal_static_com_vortex_vortexdb_backend_store_raft_rpc_ListPeersResponse_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersResponse.class, com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersResponse.Builder.class);
    }

    public static com.google.protobuf.Parser<ListPeersResponse> PARSER =
        new com.google.protobuf.AbstractParser<ListPeersResponse>() {
      public ListPeersResponse parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new ListPeersResponse(input, extensionRegistry);
      }
    };

    @Override
    public com.google.protobuf.Parser<ListPeersResponse> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    // required .com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse common = 1;
    public static final int COMMON_FIELD_NUMBER = 1;
    private com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse common_;
    /**
     * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse common = 1;</code>
     */
    public boolean hasCommon() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse common = 1;</code>
     */
    public com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse getCommon() {
      return common_;
    }
    /**
     * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse common = 1;</code>
     */
    public com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponseOrBuilder getCommonOrBuilder() {
      return common_;
    }

    // repeated string endpoints = 2;
    public static final int ENDPOINTS_FIELD_NUMBER = 2;
    private com.google.protobuf.LazyStringList endpoints_;
    /**
     * <code>repeated string endpoints = 2;</code>
     */
    public java.util.List<String>
        getEndpointsList() {
      return endpoints_;
    }
    /**
     * <code>repeated string endpoints = 2;</code>
     */
    public int getEndpointsCount() {
      return endpoints_.size();
    }
    /**
     * <code>repeated string endpoints = 2;</code>
     */
    public String getEndpoints(int index) {
      return endpoints_.get(index);
    }
    /**
     * <code>repeated string endpoints = 2;</code>
     */
    public com.google.protobuf.ByteString
        getEndpointsBytes(int index) {
      return endpoints_.getByteString(index);
    }

    private void initFields() {
      common_ = com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse.getDefaultInstance();
      endpoints_ = com.google.protobuf.LazyStringArrayList.EMPTY;
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      if (!hasCommon()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!getCommon().isInitialized()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeMessage(1, common_);
      }
      for (int i = 0; i < endpoints_.size(); i++) {
        output.writeBytes(2, endpoints_.getByteString(i));
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(1, common_);
      }
      {
        int dataSize = 0;
        for (int i = 0; i < endpoints_.size(); i++) {
          dataSize += com.google.protobuf.CodedOutputStream
            .computeBytesSizeNoTag(endpoints_.getByteString(i));
        }
        size += dataSize;
        size += 1 * getEndpointsList().size();
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @Override
    protected Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersResponse parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersResponse parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersResponse parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersResponse parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersResponse parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersResponse parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersResponse parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersResponse parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersResponse parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersResponse parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersResponse prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @Override
    protected Builder newBuilderForType(
        BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code com.vortex.vortexdb.backend.store.raft.rpc.ListPeersResponse}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersResponseOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.internal_static_com_vortex_vortexdb_backend_store_raft_rpc_ListPeersResponse_descriptor;
      }

      protected FieldAccessorTable
          internalGetFieldAccessorTable() {
        return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.internal_static_com_vortex_vortexdb_backend_store_raft_rpc_ListPeersResponse_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersResponse.class, com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersResponse.Builder.class);
      }

      // Construct using com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersResponse.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
          getCommonFieldBuilder();
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        if (commonBuilder_ == null) {
          common_ = com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse.getDefaultInstance();
        } else {
          commonBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000001);
        endpoints_ = com.google.protobuf.LazyStringArrayList.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000002);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.internal_static_com_vortex_vortexdb_backend_store_raft_rpc_ListPeersResponse_descriptor;
      }

      public com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersResponse getDefaultInstanceForType() {
        return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersResponse.getDefaultInstance();
      }

      public com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersResponse build() {
        com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersResponse result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersResponse buildPartial() {
        com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersResponse result = new com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersResponse(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        if (commonBuilder_ == null) {
          result.common_ = common_;
        } else {
          result.common_ = commonBuilder_.build();
        }
        if (((bitField0_ & 0x00000002) == 0x00000002)) {
          endpoints_ = new com.google.protobuf.UnmodifiableLazyStringList(
              endpoints_);
          bitField0_ = (bitField0_ & ~0x00000002);
        }
        result.endpoints_ = endpoints_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersResponse) {
          return mergeFrom((com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersResponse)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersResponse other) {
        if (other == com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersResponse.getDefaultInstance()) return this;
        if (other.hasCommon()) {
          mergeCommon(other.getCommon());
        }
        if (!other.endpoints_.isEmpty()) {
          if (endpoints_.isEmpty()) {
            endpoints_ = other.endpoints_;
            bitField0_ = (bitField0_ & ~0x00000002);
          } else {
            ensureEndpointsIsMutable();
            endpoints_.addAll(other.endpoints_);
          }
          onChanged();
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        if (!hasCommon()) {

          return false;
        }
        if (!getCommon().isInitialized()) {

          return false;
        }
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersResponse parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.ListPeersResponse) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // required .com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse common = 1;
      private com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse common_ = com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse.getDefaultInstance();
      private com.google.protobuf.SingleFieldBuilder<
          com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse, com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse.Builder, com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponseOrBuilder> commonBuilder_;
      /**
       * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse common = 1;</code>
       */
      public boolean hasCommon() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse common = 1;</code>
       */
      public com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse getCommon() {
        if (commonBuilder_ == null) {
          return common_;
        } else {
          return commonBuilder_.getMessage();
        }
      }
      /**
       * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse common = 1;</code>
       */
      public Builder setCommon(com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse value) {
        if (commonBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          common_ = value;
          onChanged();
        } else {
          commonBuilder_.setMessage(value);
        }
        bitField0_ |= 0x00000001;
        return this;
      }
      /**
       * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse common = 1;</code>
       */
      public Builder setCommon(
          com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse.Builder builderForValue) {
        if (commonBuilder_ == null) {
          common_ = builderForValue.build();
          onChanged();
        } else {
          commonBuilder_.setMessage(builderForValue.build());
        }
        bitField0_ |= 0x00000001;
        return this;
      }
      /**
       * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse common = 1;</code>
       */
      public Builder mergeCommon(com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse value) {
        if (commonBuilder_ == null) {
          if (((bitField0_ & 0x00000001) == 0x00000001) &&
              common_ != com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse.getDefaultInstance()) {
            common_ =
              com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse.newBuilder(common_).mergeFrom(value).buildPartial();
          } else {
            common_ = value;
          }
          onChanged();
        } else {
          commonBuilder_.mergeFrom(value);
        }
        bitField0_ |= 0x00000001;
        return this;
      }
      /**
       * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse common = 1;</code>
       */
      public Builder clearCommon() {
        if (commonBuilder_ == null) {
          common_ = com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse.getDefaultInstance();
          onChanged();
        } else {
          commonBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000001);
        return this;
      }
      /**
       * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse common = 1;</code>
       */
      public com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse.Builder getCommonBuilder() {
        bitField0_ |= 0x00000001;
        onChanged();
        return getCommonFieldBuilder().getBuilder();
      }
      /**
       * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse common = 1;</code>
       */
      public com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponseOrBuilder getCommonOrBuilder() {
        if (commonBuilder_ != null) {
          return commonBuilder_.getMessageOrBuilder();
        } else {
          return common_;
        }
      }
      /**
       * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse common = 1;</code>
       */
      private com.google.protobuf.SingleFieldBuilder<
          com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse, com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse.Builder, com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponseOrBuilder>
          getCommonFieldBuilder() {
        if (commonBuilder_ == null) {
          commonBuilder_ = new com.google.protobuf.SingleFieldBuilder<
              com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse, com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse.Builder, com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponseOrBuilder>(
                  common_,
                  getParentForChildren(),
                  isClean());
          common_ = null;
        }
        return commonBuilder_;
      }

      // repeated string endpoints = 2;
      private com.google.protobuf.LazyStringList endpoints_ = com.google.protobuf.LazyStringArrayList.EMPTY;
      private void ensureEndpointsIsMutable() {
        if (!((bitField0_ & 0x00000002) == 0x00000002)) {
          endpoints_ = new com.google.protobuf.LazyStringArrayList(endpoints_);
          bitField0_ |= 0x00000002;
         }
      }
      /**
       * <code>repeated string endpoints = 2;</code>
       */
      public java.util.List<String>
          getEndpointsList() {
        return java.util.Collections.unmodifiableList(endpoints_);
      }
      /**
       * <code>repeated string endpoints = 2;</code>
       */
      public int getEndpointsCount() {
        return endpoints_.size();
      }
      /**
       * <code>repeated string endpoints = 2;</code>
       */
      public String getEndpoints(int index) {
        return endpoints_.get(index);
      }
      /**
       * <code>repeated string endpoints = 2;</code>
       */
      public com.google.protobuf.ByteString
          getEndpointsBytes(int index) {
        return endpoints_.getByteString(index);
      }
      /**
       * <code>repeated string endpoints = 2;</code>
       */
      public Builder setEndpoints(
          int index, String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  ensureEndpointsIsMutable();
        endpoints_.set(index, value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated string endpoints = 2;</code>
       */
      public Builder addEndpoints(
          String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  ensureEndpointsIsMutable();
        endpoints_.add(value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated string endpoints = 2;</code>
       */
      public Builder addAllEndpoints(
          Iterable<String> values) {
        ensureEndpointsIsMutable();
        super.addAll(values, endpoints_);
        onChanged();
        return this;
      }
      /**
       * <code>repeated string endpoints = 2;</code>
       */
      public Builder clearEndpoints() {
        endpoints_ = com.google.protobuf.LazyStringArrayList.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000002);
        onChanged();
        return this;
      }
      /**
       * <code>repeated string endpoints = 2;</code>
       */
      public Builder addEndpointsBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  ensureEndpointsIsMutable();
        endpoints_.add(value);
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:com.vortex.vortexdb.backend.store.raft.rpc.ListPeersResponse)
    }

    static {
      defaultInstance = new ListPeersResponse(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:com.vortex.vortexdb.backend.store.raft.rpc.ListPeersResponse)
  }

  public interface SetLeaderRequestOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // required string endpoint = 1;
    /**
     * <code>required string endpoint = 1;</code>
     */
    boolean hasEndpoint();
    /**
     * <code>required string endpoint = 1;</code>
     */
    String getEndpoint();
    /**
     * <code>required string endpoint = 1;</code>
     */
    com.google.protobuf.ByteString
        getEndpointBytes();
  }
  /**
   * Protobuf type {@code com.vortex.vortexdb.backend.store.raft.rpc.SetLeaderRequest}
   */
  public static final class SetLeaderRequest extends
      com.google.protobuf.GeneratedMessage
      implements SetLeaderRequestOrBuilder {
    // Use SetLeaderRequest.newBuilder() to construct.
    private SetLeaderRequest(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private SetLeaderRequest(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final SetLeaderRequest defaultInstance;
    public static SetLeaderRequest getDefaultInstance() {
      return defaultInstance;
    }

    public SetLeaderRequest getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private SetLeaderRequest(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 10: {
              bitField0_ |= 0x00000001;
              endpoint_ = input.readBytes();
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.internal_static_com_vortex_vortexdb_backend_store_raft_rpc_SetLeaderRequest_descriptor;
    }

    protected FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.internal_static_com_vortex_vortexdb_backend_store_raft_rpc_SetLeaderRequest_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderRequest.class, com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderRequest.Builder.class);
    }

    public static com.google.protobuf.Parser<SetLeaderRequest> PARSER =
        new com.google.protobuf.AbstractParser<SetLeaderRequest>() {
      public SetLeaderRequest parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new SetLeaderRequest(input, extensionRegistry);
      }
    };

    @Override
    public com.google.protobuf.Parser<SetLeaderRequest> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    // required string endpoint = 1;
    public static final int ENDPOINT_FIELD_NUMBER = 1;
    private Object endpoint_;
    /**
     * <code>required string endpoint = 1;</code>
     */
    public boolean hasEndpoint() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>required string endpoint = 1;</code>
     */
    public String getEndpoint() {
      Object ref = endpoint_;
      if (ref instanceof String) {
        return (String) ref;
      } else {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          endpoint_ = s;
        }
        return s;
      }
    }
    /**
     * <code>required string endpoint = 1;</code>
     */
    public com.google.protobuf.ByteString
        getEndpointBytes() {
      Object ref = endpoint_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b =
            com.google.protobuf.ByteString.copyFromUtf8(
                (String) ref);
        endpoint_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    private void initFields() {
      endpoint_ = "";
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      if (!hasEndpoint()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeBytes(1, getEndpointBytes());
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(1, getEndpointBytes());
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @Override
    protected Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderRequest parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderRequest parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderRequest parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderRequest parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderRequest parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderRequest parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderRequest parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderRequest parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderRequest parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderRequest parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderRequest prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @Override
    protected Builder newBuilderForType(
        BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code com.vortex.vortexdb.backend.store.raft.rpc.SetLeaderRequest}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderRequestOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.internal_static_com_vortex_vortexdb_backend_store_raft_rpc_SetLeaderRequest_descriptor;
      }

      protected FieldAccessorTable
          internalGetFieldAccessorTable() {
        return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.internal_static_com_vortex_vortexdb_backend_store_raft_rpc_SetLeaderRequest_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderRequest.class, com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderRequest.Builder.class);
      }

      // Construct using com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderRequest.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        endpoint_ = "";
        bitField0_ = (bitField0_ & ~0x00000001);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.internal_static_com_vortex_vortexdb_backend_store_raft_rpc_SetLeaderRequest_descriptor;
      }

      public com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderRequest getDefaultInstanceForType() {
        return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderRequest.getDefaultInstance();
      }

      public com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderRequest build() {
        com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderRequest result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderRequest buildPartial() {
        com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderRequest result = new com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderRequest(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.endpoint_ = endpoint_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderRequest) {
          return mergeFrom((com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderRequest)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderRequest other) {
        if (other == com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderRequest.getDefaultInstance()) return this;
        if (other.hasEndpoint()) {
          bitField0_ |= 0x00000001;
          endpoint_ = other.endpoint_;
          onChanged();
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        if (!hasEndpoint()) {

          return false;
        }
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderRequest parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderRequest) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // required string endpoint = 1;
      private Object endpoint_ = "";
      /**
       * <code>required string endpoint = 1;</code>
       */
      public boolean hasEndpoint() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>required string endpoint = 1;</code>
       */
      public String getEndpoint() {
        Object ref = endpoint_;
        if (!(ref instanceof String)) {
          String s = ((com.google.protobuf.ByteString) ref)
              .toStringUtf8();
          endpoint_ = s;
          return s;
        } else {
          return (String) ref;
        }
      }
      /**
       * <code>required string endpoint = 1;</code>
       */
      public com.google.protobuf.ByteString
          getEndpointBytes() {
        Object ref = endpoint_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b =
              com.google.protobuf.ByteString.copyFromUtf8(
                  (String) ref);
          endpoint_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>required string endpoint = 1;</code>
       */
      public Builder setEndpoint(
          String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        endpoint_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required string endpoint = 1;</code>
       */
      public Builder clearEndpoint() {
        bitField0_ = (bitField0_ & ~0x00000001);
        endpoint_ = getDefaultInstance().getEndpoint();
        onChanged();
        return this;
      }
      /**
       * <code>required string endpoint = 1;</code>
       */
      public Builder setEndpointBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        endpoint_ = value;
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:com.vortex.vortexdb.backend.store.raft.rpc.SetLeaderRequest)
    }

    static {
      defaultInstance = new SetLeaderRequest(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:com.vortex.vortexdb.backend.store.raft.rpc.SetLeaderRequest)
  }

  public interface SetLeaderResponseOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // required .com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse common = 1;
    /**
     * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse common = 1;</code>
     */
    boolean hasCommon();
    /**
     * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse common = 1;</code>
     */
    com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse getCommon();
    /**
     * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse common = 1;</code>
     */
    com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponseOrBuilder getCommonOrBuilder();
  }
  /**
   * Protobuf type {@code com.vortex.vortexdb.backend.store.raft.rpc.SetLeaderResponse}
   */
  public static final class SetLeaderResponse extends
      com.google.protobuf.GeneratedMessage
      implements SetLeaderResponseOrBuilder {
    // Use SetLeaderResponse.newBuilder() to construct.
    private SetLeaderResponse(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private SetLeaderResponse(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final SetLeaderResponse defaultInstance;
    public static SetLeaderResponse getDefaultInstance() {
      return defaultInstance;
    }

    public SetLeaderResponse getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private SetLeaderResponse(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 10: {
              com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse.Builder subBuilder = null;
              if (((bitField0_ & 0x00000001) == 0x00000001)) {
                subBuilder = common_.toBuilder();
              }
              common_ = input.readMessage(com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse.PARSER, extensionRegistry);
              if (subBuilder != null) {
                subBuilder.mergeFrom(common_);
                common_ = subBuilder.buildPartial();
              }
              bitField0_ |= 0x00000001;
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.internal_static_com_vortex_vortexdb_backend_store_raft_rpc_SetLeaderResponse_descriptor;
    }

    protected FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.internal_static_com_vortex_vortexdb_backend_store_raft_rpc_SetLeaderResponse_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderResponse.class, com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderResponse.Builder.class);
    }

    public static com.google.protobuf.Parser<SetLeaderResponse> PARSER =
        new com.google.protobuf.AbstractParser<SetLeaderResponse>() {
      public SetLeaderResponse parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new SetLeaderResponse(input, extensionRegistry);
      }
    };

    @Override
    public com.google.protobuf.Parser<SetLeaderResponse> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    // required .com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse common = 1;
    public static final int COMMON_FIELD_NUMBER = 1;
    private com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse common_;
    /**
     * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse common = 1;</code>
     */
    public boolean hasCommon() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse common = 1;</code>
     */
    public com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse getCommon() {
      return common_;
    }
    /**
     * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse common = 1;</code>
     */
    public com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponseOrBuilder getCommonOrBuilder() {
      return common_;
    }

    private void initFields() {
      common_ = com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse.getDefaultInstance();
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      if (!hasCommon()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!getCommon().isInitialized()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeMessage(1, common_);
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(1, common_);
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @Override
    protected Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderResponse parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderResponse parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderResponse parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderResponse parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderResponse parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderResponse parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderResponse parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderResponse parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderResponse parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderResponse parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderResponse prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @Override
    protected Builder newBuilderForType(
        BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code com.vortex.vortexdb.backend.store.raft.rpc.SetLeaderResponse}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderResponseOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.internal_static_com_vortex_vortexdb_backend_store_raft_rpc_SetLeaderResponse_descriptor;
      }

      protected FieldAccessorTable
          internalGetFieldAccessorTable() {
        return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.internal_static_com_vortex_vortexdb_backend_store_raft_rpc_SetLeaderResponse_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderResponse.class, com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderResponse.Builder.class);
      }

      // Construct using com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderResponse.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
          getCommonFieldBuilder();
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        if (commonBuilder_ == null) {
          common_ = com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse.getDefaultInstance();
        } else {
          commonBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000001);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.internal_static_com_vortex_vortexdb_backend_store_raft_rpc_SetLeaderResponse_descriptor;
      }

      public com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderResponse getDefaultInstanceForType() {
        return com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderResponse.getDefaultInstance();
      }

      public com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderResponse build() {
        com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderResponse result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderResponse buildPartial() {
        com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderResponse result = new com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderResponse(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        if (commonBuilder_ == null) {
          result.common_ = common_;
        } else {
          result.common_ = commonBuilder_.build();
        }
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderResponse) {
          return mergeFrom((com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderResponse)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderResponse other) {
        if (other == com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderResponse.getDefaultInstance()) return this;
        if (other.hasCommon()) {
          mergeCommon(other.getCommon());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        if (!hasCommon()) {

          return false;
        }
        if (!getCommon().isInitialized()) {

          return false;
        }
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderResponse parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.SetLeaderResponse) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // required .com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse common = 1;
      private com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse common_ = com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse.getDefaultInstance();
      private com.google.protobuf.SingleFieldBuilder<
          com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse, com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse.Builder, com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponseOrBuilder> commonBuilder_;
      /**
       * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse common = 1;</code>
       */
      public boolean hasCommon() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse common = 1;</code>
       */
      public com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse getCommon() {
        if (commonBuilder_ == null) {
          return common_;
        } else {
          return commonBuilder_.getMessage();
        }
      }
      /**
       * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse common = 1;</code>
       */
      public Builder setCommon(com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse value) {
        if (commonBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          common_ = value;
          onChanged();
        } else {
          commonBuilder_.setMessage(value);
        }
        bitField0_ |= 0x00000001;
        return this;
      }
      /**
       * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse common = 1;</code>
       */
      public Builder setCommon(
          com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse.Builder builderForValue) {
        if (commonBuilder_ == null) {
          common_ = builderForValue.build();
          onChanged();
        } else {
          commonBuilder_.setMessage(builderForValue.build());
        }
        bitField0_ |= 0x00000001;
        return this;
      }
      /**
       * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse common = 1;</code>
       */
      public Builder mergeCommon(com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse value) {
        if (commonBuilder_ == null) {
          if (((bitField0_ & 0x00000001) == 0x00000001) &&
              common_ != com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse.getDefaultInstance()) {
            common_ =
              com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse.newBuilder(common_).mergeFrom(value).buildPartial();
          } else {
            common_ = value;
          }
          onChanged();
        } else {
          commonBuilder_.mergeFrom(value);
        }
        bitField0_ |= 0x00000001;
        return this;
      }
      /**
       * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse common = 1;</code>
       */
      public Builder clearCommon() {
        if (commonBuilder_ == null) {
          common_ = com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse.getDefaultInstance();
          onChanged();
        } else {
          commonBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000001);
        return this;
      }
      /**
       * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse common = 1;</code>
       */
      public com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse.Builder getCommonBuilder() {
        bitField0_ |= 0x00000001;
        onChanged();
        return getCommonFieldBuilder().getBuilder();
      }
      /**
       * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse common = 1;</code>
       */
      public com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponseOrBuilder getCommonOrBuilder() {
        if (commonBuilder_ != null) {
          return commonBuilder_.getMessageOrBuilder();
        } else {
          return common_;
        }
      }
      /**
       * <code>required .com.vortex.vortexdb.backend.store.raft.rpc.CommonResponse common = 1;</code>
       */
      private com.google.protobuf.SingleFieldBuilder<
          com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse, com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse.Builder, com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponseOrBuilder>
          getCommonFieldBuilder() {
        if (commonBuilder_ == null) {
          commonBuilder_ = new com.google.protobuf.SingleFieldBuilder<
              com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse, com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponse.Builder, com.vortex.vortexdb.backend.store.raft.rpc.RaftRequests.CommonResponseOrBuilder>(
                  common_,
                  getParentForChildren(),
                  isClean());
          common_ = null;
        }
        return commonBuilder_;
      }

      // @@protoc_insertion_point(builder_scope:com.vortex.vortexdb.backend.store.raft.rpc.SetLeaderResponse)
    }

    static {
      defaultInstance = new SetLeaderResponse(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:com.vortex.vortexdb.backend.store.raft.rpc.SetLeaderResponse)
  }

  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_com_vortex_vortexdb_backend_store_raft_rpc_StoreCommandRequest_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_com_vortex_vortexdb_backend_store_raft_rpc_StoreCommandRequest_fieldAccessorTable;
  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_com_vortex_vortexdb_backend_store_raft_rpc_StoreCommandResponse_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_com_vortex_vortexdb_backend_store_raft_rpc_StoreCommandResponse_fieldAccessorTable;
  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_com_vortex_vortexdb_backend_store_raft_rpc_CommonResponse_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_com_vortex_vortexdb_backend_store_raft_rpc_CommonResponse_fieldAccessorTable;
  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_com_vortex_vortexdb_backend_store_raft_rpc_ListPeersRequest_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_com_vortex_vortexdb_backend_store_raft_rpc_ListPeersRequest_fieldAccessorTable;
  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_com_vortex_vortexdb_backend_store_raft_rpc_ListPeersResponse_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_com_vortex_vortexdb_backend_store_raft_rpc_ListPeersResponse_fieldAccessorTable;
  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_com_vortex_vortexdb_backend_store_raft_rpc_SetLeaderRequest_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_com_vortex_vortexdb_backend_store_raft_rpc_SetLeaderRequest_fieldAccessorTable;
  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_com_vortex_vortexdb_backend_store_raft_rpc_SetLeaderResponse_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_com_vortex_vortexdb_backend_store_raft_rpc_SetLeaderResponse_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    String[] descriptorData = {
      "\n,vortex-core/src/main/resources/raft" +
      ".proto\022*com.vortex.vortexdb.backend.stor" +
      "e.raft.rpc\"\261\001\n\023StoreCommandRequest\022C\n\004ty" +
      "pe\030\001 \002(\01625.com.vortex.vortexdb.backend.s" +
      "tore.raft.rpc.StoreType\022G\n\006action\030\002 \002(\0162" +
      "7.com.vortex.vortexdb.backend.store.raft" +
      ".rpc.StoreAction\022\014\n\004data\030\003 \002(\014\"7\n\024StoreC" +
      "ommandResponse\022\016\n\006status\030\001 \002(\010\022\017\n\007messag" +
      "e\030\002 \001(\t\"1\n\016CommonResponse\022\016\n\006status\030\001 \002(" +
      "\010\022\017\n\007message\030\002 \001(\t\"\022\n\020ListPeersRequest\"r",
      "\n\021ListPeersResponse\022J\n\006common\030\001 \002(\0132:.co" +
      "m.vortex.vortexdb.backend.store.raft.rpc" +
      ".CommonResponse\022\021\n\tendpoints\030\002 \003(\t\"$\n\020Se" +
      "tLeaderRequest\022\020\n\010endpoint\030\001 \002(\t\"_\n\021SetL" +
      "eaderResponse\022J\n\006common\030\001 \002(\0132:.com.vortex" +
      "x.vortexdb.backend.store.raft.rpc.Commo" +
      "nResponse*7\n\tStoreType\022\n\n\006SCHEMA\020\000\022\t\n\005GR" +
      "APH\020\001\022\n\n\006SYSTEM\020\002\022\007\n\003ALL\020\003*\237\001\n\013StoreActi" +
      "on\022\010\n\004NONE\020\000\022\010\n\004INIT\020\001\022\t\n\005CLEAR\020\002\022\014\n\010TRU" +
      "NCATE\020\003\022\014\n\010SNAPSHOT\020\004\022\014\n\010BEGIN_TX\020\n\022\r\n\tC",
      "OMMIT_TX\020\013\022\017\n\013ROLLBACK_TX\020\014\022\n\n\006MUTATE\020\024\022" +
      "\020\n\014INCR_COUNTER\020\025\022\t\n\005QUERY\020\036B:\n*com.vorte" +
      "x.vortexdb.backend.store.raft.rpcB\014Raft" +
      "Requests"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
      new com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner() {
        public com.google.protobuf.ExtensionRegistry assignDescriptors(
            com.google.protobuf.Descriptors.FileDescriptor root) {
          descriptor = root;
          internal_static_com_vortex_vortexdb_backend_store_raft_rpc_StoreCommandRequest_descriptor =
            getDescriptor().getMessageTypes().get(0);
          internal_static_com_vortex_vortexdb_backend_store_raft_rpc_StoreCommandRequest_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_com_vortex_vortexdb_backend_store_raft_rpc_StoreCommandRequest_descriptor,
              new String[] { "Type", "Action", "Data", });
          internal_static_com_vortex_vortexdb_backend_store_raft_rpc_StoreCommandResponse_descriptor =
            getDescriptor().getMessageTypes().get(1);
          internal_static_com_vortex_vortexdb_backend_store_raft_rpc_StoreCommandResponse_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_com_vortex_vortexdb_backend_store_raft_rpc_StoreCommandResponse_descriptor,
              new String[] { "Status", "Message", });
          internal_static_com_vortex_vortexdb_backend_store_raft_rpc_CommonResponse_descriptor =
            getDescriptor().getMessageTypes().get(2);
          internal_static_com_vortex_vortexdb_backend_store_raft_rpc_CommonResponse_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_com_vortex_vortexdb_backend_store_raft_rpc_CommonResponse_descriptor,
              new String[] { "Status", "Message", });
          internal_static_com_vortex_vortexdb_backend_store_raft_rpc_ListPeersRequest_descriptor =
            getDescriptor().getMessageTypes().get(3);
          internal_static_com_vortex_vortexdb_backend_store_raft_rpc_ListPeersRequest_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_com_vortex_vortexdb_backend_store_raft_rpc_ListPeersRequest_descriptor,
              new String[] { });
          internal_static_com_vortex_vortexdb_backend_store_raft_rpc_ListPeersResponse_descriptor =
            getDescriptor().getMessageTypes().get(4);
          internal_static_com_vortex_vortexdb_backend_store_raft_rpc_ListPeersResponse_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_com_vortex_vortexdb_backend_store_raft_rpc_ListPeersResponse_descriptor,
              new String[] { "Common", "Endpoints", });
          internal_static_com_vortex_vortexdb_backend_store_raft_rpc_SetLeaderRequest_descriptor =
            getDescriptor().getMessageTypes().get(5);
          internal_static_com_vortex_vortexdb_backend_store_raft_rpc_SetLeaderRequest_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_com_vortex_vortexdb_backend_store_raft_rpc_SetLeaderRequest_descriptor,
              new String[] { "Endpoint", });
          internal_static_com_vortex_vortexdb_backend_store_raft_rpc_SetLeaderResponse_descriptor =
            getDescriptor().getMessageTypes().get(6);
          internal_static_com_vortex_vortexdb_backend_store_raft_rpc_SetLeaderResponse_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_com_vortex_vortexdb_backend_store_raft_rpc_SetLeaderResponse_descriptor,
              new String[] { "Common", });
          return null;
        }
      };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
  }

  // @@protoc_insertion_point(outer_class_scope)
}

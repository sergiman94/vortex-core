package com.vortex.client.structure.schema;

import com.vortex.client.structure.SchemaElement;

public interface SchemaBuilder<T extends SchemaElement> {

    T build();

    T create();

    T append();

    T eliminate();

    void remove();
}

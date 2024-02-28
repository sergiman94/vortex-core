
package com.vortex.vortexdb.schema.builder;

import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.schema.SchemaElement;

public interface SchemaBuilder<T extends SchemaElement> {

    public SchemaBuilder<T> id(long id);

    public T build();

    public T create();

    public T append();

    public T eliminate();

    public Id remove();

    public SchemaBuilder<T> ifNotExist();

    public SchemaBuilder<T> checkExist(boolean checkExist);
}

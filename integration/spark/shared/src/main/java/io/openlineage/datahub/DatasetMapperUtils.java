package io.openlineage.datahub;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.schema.NumberType;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.StringType;
import io.openlineage.client.OpenLineage;

public class DatasetMapperUtils {
    public static String getPlatform(OpenLineage.Dataset dataset) {
        // TODO = handle all types of namespaces
        return dataset.getNamespace().contains("postgres") ? "posgtres" : dataset.getNamespace();
    }

    public static DatasetUrn getUrn(OpenLineage.Dataset input) {
        return new DatasetUrn(new DataPlatformUrn(getPlatform(input)), input.getName(), FabricType.NON_PROD);
    }

    public static SchemaField getSchemaField(OpenLineage.SchemaDatasetFacetFields f) {
        SchemaFieldDataType.Type value;
        // TODO handle different datatypes
        if("integer".equals(f.getType())){
            value = SchemaFieldDataType.Type.create(new NumberType());
        }
        else {
            value = SchemaFieldDataType.Type.create(new StringType());
        }
        return new SchemaField().setFieldPath(f.getName()).setType(new SchemaFieldDataType().setType(value)).setNativeDataType(f.getType());
    }
    
}

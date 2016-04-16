package com.hzih.itp.platform.dbchange.target.info;


import com.hzih.itp.platform.dbchange.datautils.db.Field;

/**
 * Created by IntelliJ IDEA.
 * User: wxh
 * Date: 2005-7-11
 * Time: 15:19:06
 * To change this template use File | Settings | File Templates.
 */
public class FieldMap {


    private String sourceField;
    private Field targetField;

    public FieldMap(String sf, Field c) {
        sourceField = sf;
        targetField = c;
    }

    public String getSourceField() {
        return sourceField;
    }

    public String getTargetField() {
        return targetField.getName();
    }

    public Field getTargetColumn() {
        return targetField;
    }


}

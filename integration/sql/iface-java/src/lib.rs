// Copyright 2018-2026 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

extern crate jni;
extern crate openlineage_sql as rust_impl;

use anyhow::Result;
use jni::errors::Error;
use jni::objects::{JClass, JList, JObject, JString, JValue, JValueOwned};
use jni::signature::{MethodSignature, RuntimeMethodSignature};
use jni::strings::JNIString;
use jni::sys::{jobject, jstring};
use jni::{Env, EnvUnowned, Outcome};

use rust_impl::{get_generic_dialect, parse_multiple_statements};

trait AsJavaObject {
    fn as_java_object<'local>(&self, env: &mut Env<'local>) -> Result<JObject<'local>> {
        let rt_sig = RuntimeMethodSignature::from_str(Self::ctor_signature())?;
        let sig = MethodSignature::from(&rt_sig);
        let args_owned = self.ctor_arguments(env)?;
        let args: Vec<JValue<'_>> = args_owned.iter().map(|v| v.borrow()).collect();
        let obj = env.new_object(JNIString::from(Self::java_class_name()), sig, &args)?;
        Ok(obj)
    }

    fn java_class_name() -> &'static str;
    fn ctor_signature() -> &'static str;
    fn ctor_arguments<'local>(&self, env: &mut Env<'local>) -> Result<Vec<JValueOwned<'local>>>;
}

impl AsJavaObject for rust_impl::SqlMeta {
    fn java_class_name() -> &'static str {
        "io/openlineage/sql/SqlMeta"
    }

    fn ctor_signature() -> &'static str {
        "(Ljava/util/List;Ljava/util/List;Ljava/util/List;Ljava/util/List;)V"
    }

    fn ctor_arguments<'local>(&self, env: &mut Env<'local>) -> Result<Vec<JValueOwned<'local>>> {
        let empty_sig = RuntimeMethodSignature::from_str("()V")?;
        let empty = MethodSignature::from(&empty_sig);
        let array_list = JNIString::from("java/util/ArrayList");

        let ins_raw = env.new_object(array_list.clone(), &empty, &[])?;
        let outs_raw = env.new_object(array_list.clone(), &empty, &[])?;
        let columns_raw = env.new_object(array_list.clone(), &empty, &[])?;
        let errors_raw = env.new_object(array_list, &empty, &[])?;

        let ins = env.cast_local::<JList>(ins_raw)?;
        let outs = env.cast_local::<JList>(outs_raw)?;
        let columns = env.cast_local::<JList>(columns_raw)?;
        let errors = env.cast_local::<JList>(errors_raw)?;

        for e in &self.table_lineage.in_tables {
            let obj = e.as_java_object(env)?;
            ins.add(env, &obj)?;
        }
        for e in &self.table_lineage.out_tables {
            let obj = e.as_java_object(env)?;
            outs.add(env, &obj)?;
        }
        for e in &self.column_lineage {
            let obj = e.as_java_object(env)?;
            columns.add(env, &obj)?;
        }
        for e in &self.errors {
            let obj = e.as_java_object(env)?;
            errors.add(env, &obj)?;
        }

        Ok(vec![
            JValueOwned::Object(ins.into()),
            JValueOwned::Object(outs.into()),
            JValueOwned::Object(columns.into()),
            JValueOwned::Object(errors.into()),
        ])
    }
}

impl AsJavaObject for rust_impl::QuoteStyle {
    fn java_class_name() -> &'static str {
        "io/openlineage/sql/QuoteStyle"
    }

    fn ctor_signature() -> &'static str {
        "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V"
    }

    fn ctor_arguments<'local>(&self, env: &mut Env<'local>) -> Result<Vec<JValueOwned<'local>>> {
        let arg1: JObject<'local> = match &self.database {
            Some(n) => env.new_string(n.to_string())?.into(),
            None => JObject::null(),
        };
        let arg2: JObject<'local> = match &self.schema {
            Some(n) => env.new_string(n.to_string())?.into(),
            None => JObject::null(),
        };
        let arg3: JObject<'local> = match &self.name {
            Some(n) => env.new_string(n.to_string())?.into(),
            None => JObject::null(),
        };

        Ok(vec![
            JValueOwned::Object(arg1),
            JValueOwned::Object(arg2),
            JValueOwned::Object(arg3),
        ])
    }
}

impl AsJavaObject for rust_impl::DbTableMeta {
    fn java_class_name() -> &'static str {
        "io/openlineage/sql/DbTableMeta"
    }

    fn ctor_signature() -> &'static str {
        "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Lio/openlineage/sql/QuoteStyle;)V"
    }

    fn ctor_arguments<'local>(&self, env: &mut Env<'local>) -> Result<Vec<JValueOwned<'local>>> {
        let arg1: JObject<'local> = match &self.database {
            Some(d) => env.new_string(d)?.into(),
            None => JObject::null(),
        };
        let arg2: JObject<'local> = match &self.schema {
            Some(s) => env.new_string(s)?.into(),
            None => JObject::null(),
        };
        let arg3: JObject<'local> = env.new_string(&self.name)?.into();

        let arg4: JObject<'local> = match &self.quote_style {
            Some(q) => q.as_java_object(env)?,
            None => JObject::null(),
        };

        Ok(vec![
            JValueOwned::Object(arg1),
            JValueOwned::Object(arg2),
            JValueOwned::Object(arg3),
            JValueOwned::Object(arg4),
        ])
    }
}

impl AsJavaObject for rust_impl::ColumnMeta {
    fn java_class_name() -> &'static str {
        "io/openlineage/sql/ColumnMeta"
    }

    fn ctor_signature() -> &'static str {
        "(Lio/openlineage/sql/DbTableMeta;Ljava/lang/String;)V"
    }

    fn ctor_arguments<'local>(&self, env: &mut Env<'local>) -> Result<Vec<JValueOwned<'local>>> {
        let arg1: JObject<'local> = match &self.origin {
            Some(d) => d.as_java_object(env)?,
            None => JObject::null(),
        };
        let arg2: JObject<'local> = env.new_string(&self.name)?.into();

        Ok(vec![JValueOwned::Object(arg1), JValueOwned::Object(arg2)])
    }
}

impl AsJavaObject for rust_impl::ColumnLineage {
    fn java_class_name() -> &'static str {
        "io/openlineage/sql/ColumnLineage"
    }

    fn ctor_signature() -> &'static str {
        "(Lio/openlineage/sql/ColumnMeta;Ljava/util/List;)V"
    }

    fn ctor_arguments<'local>(&self, env: &mut Env<'local>) -> Result<Vec<JValueOwned<'local>>> {
        let empty_sig = RuntimeMethodSignature::from_str("()V")?;
        let empty = MethodSignature::from(&empty_sig);

        let lineage_raw = env.new_object(JNIString::from("java/util/ArrayList"), &empty, &[])?;
        let lineage = env.cast_local::<JList>(lineage_raw)?;

        for e in &self.lineage {
            let obj = e.as_java_object(env)?;
            lineage.add(env, &obj)?;
        }

        let descendant = self.descendant.as_java_object(env)?;

        Ok(vec![
            JValueOwned::Object(descendant),
            JValueOwned::Object(lineage.into()),
        ])
    }
}

impl AsJavaObject for rust_impl::ExtractionError {
    fn java_class_name() -> &'static str {
        "io/openlineage/sql/ExtractionError"
    }

    fn ctor_signature() -> &'static str {
        "(JLjava/lang/String;Ljava/lang/String;)V"
    }

    fn ctor_arguments<'local>(&self, env: &mut Env<'local>) -> Result<Vec<JValueOwned<'local>>> {
        let message: JObject<'local> = env.new_string(&self.message)?.into();
        let origin_statement: JObject<'local> = env.new_string(&self.origin_statement)?.into();

        Ok(vec![
            JValueOwned::Long(self.index as i64),
            JValueOwned::Object(message),
            JValueOwned::Object(origin_statement),
        ])
    }
}

fn throw_anyhow(env: &mut Env<'_>, err: anyhow::Error) {
    let _ = env.throw_new(
        JNIString::from("java/lang/RuntimeException"),
        JNIString::from(err.to_string()),
    );
}

#[no_mangle]
pub extern "system" fn Java_io_openlineage_sql_OpenLineageSql_parse(
    mut env: EnvUnowned,
    _class: JClass,
    sql: JObject,
    dialect: JString,
    default_schema: JString,
) -> jobject {
    let outcome = env.with_env(move |env| -> Result<jobject, Error> {
        let result: anyhow::Result<jobject> = (|| {
            let sql_list = env.cast_local::<JList>(sql)?;
            let mut vec_sql: Vec<String> = vec![];
            let size = sql_list.size(env)?;
            for i in 0..size {
                let item = sql_list.get(env, i)?;
                if item.is_null() {
                    continue;
                }
                let jstr = env.cast_local::<JString>(item)?;
                let s = jstr.try_to_string(env)?;
                vec_sql.push(s);
            }

            let dialect_str: Option<String> = if dialect.as_raw().is_null() {
                None
            } else {
                Some(dialect.try_to_string(env)?)
            };
            let dialect_obj = get_generic_dialect(dialect_str);

            let default_schema_str: Option<String> = if default_schema.as_raw().is_null() {
                None
            } else {
                Some(default_schema.try_to_string(env)?)
            };

            let parsed = parse_multiple_statements(vec_sql, dialect_obj, default_schema_str)?;
            Ok(parsed.as_java_object(env)?.into_raw())
        })();

        match result {
            Ok(obj) => Ok(obj),
            Err(err) => {
                throw_anyhow(env, err);
                Ok(JObject::null().into_raw())
            }
        }
    });

    match outcome.into_outcome() {
        Outcome::Ok(obj) => obj,
        Outcome::Err(_) | Outcome::Panic(_) => JObject::null().into_raw(),
    }
}

#[no_mangle]
pub extern "system" fn Java_io_openlineage_sql_OpenLineageSql_provider(
    mut env: EnvUnowned,
    _class: JClass,
) -> jstring {
    match env
        .with_env(|env| -> Result<jstring, Error> { Ok(env.new_string("rust")?.into_raw()) })
        .into_outcome()
    {
        Outcome::Ok(s) => s,
        Outcome::Err(_) | Outcome::Panic(_) => JObject::null().into_raw() as jstring,
    }
}

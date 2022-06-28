/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark32.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import lombok.NonNull;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.AddColumns;
import org.apache.spark.sql.catalyst.plans.logical.AlterColumn;
import org.apache.spark.sql.catalyst.plans.logical.CommentOnTable;
import org.apache.spark.sql.catalyst.plans.logical.DropColumns;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.RenameColumn;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceColumns;
import org.apache.spark.sql.catalyst.plans.logical.SetTableLocation;
import org.apache.spark.sql.catalyst.plans.logical.SetTableProperties;
import org.apache.spark.sql.catalyst.plans.logical.UnsetTableProperties;

import java.util.Collections;
import java.util.List;

public class AlterTableDatasetBuilder extends AbstractQueryPlanOutputDatasetBuilder<LogicalPlan> {

    public AlterTableDatasetBuilder(@NonNull OpenLineageContext context) {
        super(context, false);
    }

    @Override
    public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
        return  x instanceof CommentOnTable ||
                x instanceof SetTableLocation ||
                x instanceof SetTableProperties ||
                x instanceof UnsetTableProperties ||
                x instanceof AddColumns ||
                x instanceof ReplaceColumns ||
                x instanceof DropColumns ||
                x instanceof RenameColumn ||
                x instanceof AlterColumn;
    }

    @Override
    protected List<OpenLineage.OutputDataset> apply(SparkListenerEvent event, LogicalPlan alterTableCommand) {

      if (alterTableCommand instanceof CommentOnTable){

      }
      else if (alterTableCommand instanceof SetTableLocation){

      }
      else if (alterTableCommand instanceof SetTableProperties){

      }
      else if (alterTableCommand instanceof UnsetTableProperties){

      }
      else if (alterTableCommand instanceof AddColumns){

      }
      else if (alterTableCommand instanceof ReplaceColumns){

      }
      else if (alterTableCommand instanceof DropColumns){

      }
      else if (alterTableCommand instanceof RenameColumn){

      }
      else if (alterTableCommand instanceof AlterColumn){

      }
      return Collections.emptyList();
    }
}

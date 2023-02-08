package org.zzt.service;

import org.springframework.stereotype.Service
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, EqualNullSafe, EqualTo, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan, Project, Sample, SubqueryAlias, Union}
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.mv.{SchemaRegistry, TableHolder}
import org.apache.spark.sql.mv.optimizer.PreOptimizeRewrite
import org.springframework.beans.factory.annotation.Autowired
import org.zzt.entity.TableInfo.Edge
import org.zzt.entity.{Metadata, TableInfo}
import org.zzt.index.{Index, Item, RangeItem}

import scala.collection.mutable.ArrayBuffer;

@Service
@Autowired
class SparkService (metadataService: MetadataService) {
    private final val host: String = "localhost"
    private final val hdfs: String = String.format("hdfs://%s:9000", host)
    private final val hiveMetastore = String.format("thrift://%s:9083", host)

    val spark: SparkSession = SparkSession.builder()
      .appName("SparkTpchSQL")
      .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("hive.metastore.uris", hiveMetastore)
      .config("spark.sql.warehouse.dir", hdfs + "/zzt/data")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    def loadAll(): java.util.List[TableInfo] = {
        val all = metadataService.loadAllMeta()
        val ret: java.util.List[TableInfo] = new java.util.ArrayList[TableInfo]()
        all.forEach{
            f => {
                val sql = f.createSql
                println("sql:" + sql)
                val tableInfo = getTableInfo(sql)
                tableInfo.name = f.getName
                ret.add(tableInfo)
//                val lp = optimize(sql)
//                val output: Seq[String] = lp.output.map{f => f.name}
//                val tables: List[String] = extractTablesFromPlan(lp)
//
//                var viewConjunctivePredicates: Seq[Expression] = Seq()
//                var viewProjectList: Seq[Expression] = Seq()
//                var viewGroupingExpressions: Seq[Expression] = Seq()
//                var viewAggregateExpressions: Seq[Expression] = Seq()
//
//                val viewNormalizePlan = normalizePlan(lp)
//                viewNormalizePlan transformDown {
//                    case a@Filter(condition, _) =>
//                        viewConjunctivePredicates ++= splitConjunctivePredicates(condition)
//                        a
//                }
//
//                viewNormalizePlan match {
//                    case Project(projectList, Filter(condition, _)) =>
//                        viewConjunctivePredicates = splitConjunctivePredicates(condition)
//                        viewProjectList = projectList
//                    case Project(projectList, _) =>
//                        viewProjectList = projectList
//
//                    case Aggregate(groupingExpressions, aggregateExpressions, Filter(condition, _)) =>
//                        viewConjunctivePredicates = splitConjunctivePredicates(condition)
//                        viewGroupingExpressions = groupingExpressions
//                        viewAggregateExpressions = aggregateExpressions
//
//                    case Aggregate(groupingExpressions, aggregateExpressions, _) =>
//                        viewGroupingExpressions = groupingExpressions
//                        viewAggregateExpressions = aggregateExpressions
//                }
//
//                val predicates: Seq[String] = viewConjunctivePredicates.map(f => f.toString())
//                val tableInfo: TableInfo = new TableInfo()
//                tableInfo.name = f.name
//                tableInfo.level1 = new Item()
//                tableInfo.level3 = new RangeItem()
//                tables.foreach{
//                    t => tableInfo.level1.data.add(t)
//                }
//
//                tableInfo.level2 = new Item()
//                output.foreach{
//                    o => tableInfo.level2.data.add(o)
//                }
//
//                val equals: java.util.Map[java.util.Set[String], Range] = new java.util.HashMap[java.util.Set[String], Range]()
//                viewConjunctivePredicates.foreach{
//                    exp => println(exp.toString())
//                }
//                ret.add(tableInfo)
            }
        }
        // val index = new Index(ret)
        println("spark service loadAll finished")
        ret
    }

    def optimize(sql: String): LogicalPlan = {
        val schemaRegistry = new SchemaRegistry(spark)
        val lp = schemaRegistry.toLogicalPlan(sql)
        PreOptimizeRewrite.execute(lp)
    }

    def getTableInfo(sql: String): TableInfo = {
        val lp = optimize(sql)
        // check if SPJG
        var currentPlan: LogicalPlan = null
        if (isSPJG(lp)) {
            currentPlan = lp
        } else {
            lp.transformUp {
                case a if isSPJG(a) => {
                    currentPlan = a
                    a
                }
            }
        }

        val tables: List[String] = extractTablesFromPlan(currentPlan)
        val output: Seq[String] = currentPlan.output.map { f => f.name }
        val tableInfo: TableInfo = new TableInfo()
        tableInfo.sql = sql
        tables.foreach {
            t => tableInfo.level1.data.add(t)
        }
        output.foreach {
            o => tableInfo.level2.data.add(o)
        }
        // TODO level3
        // add equalJoin
        currentPlan.transformDown {
            case a@Join(_, _, _, condition, _) =>
                if (condition.isDefined) {
                    condition.get match {
                        case eq: EqualTo =>
                            println(eq)
                            tableInfo.equalJoins.add(
                                new Edge(eq.left.asInstanceOf[AttributeReference].name,
                                    eq.right.asInstanceOf[AttributeReference].name));
                        case and: And =>
                            println(and)
                            val left = and.left.asInstanceOf[EqualTo]
                            val right = and.right.asInstanceOf[EqualTo]
                            tableInfo.equalJoins.add(
                                new Edge(left.left.asInstanceOf[AttributeReference].name,
                                    left.right.asInstanceOf[AttributeReference].name));
                            tableInfo.equalJoins.add(
                                new Edge(right.left.asInstanceOf[AttributeReference].name,
                                    right.right.asInstanceOf[AttributeReference].name));
                    }
                }
                a
        }
        tableInfo
    }

    def extractTablesFromPlan(plan: LogicalPlan) = {
        extractTableHolderFromPlan(plan).map { holder =>
            if (holder.db != null) holder.db + "." + holder.table
            else holder.table
        }.filterNot(f => f == null)
    }

    def extractTableHolderFromPlan(plan: LogicalPlan) = {
        var tables = Set[TableHolder]()
        plan transformDown {
            case a@SubqueryAlias(_, LogicalRelation(_, _, _, _)) =>
                tables += TableHolder(null, a.identifier.toString, a.output, a)
                a
            case a@SubqueryAlias(_, LogicalRDD(_, _, _, _, _)) =>
                tables += TableHolder(null, a.identifier.toString, a.output, a)
                a
            case a@SubqueryAlias(_, m@HiveTableRelation(tableMeta, _, _, _, _)) =>
                tables += TableHolder(null, a.identifier.toString, a.output, a)
                a
        }
        tables.toList
    }


    def extractAttributeReferenceFromFirstLevel(exprs: Seq[Expression]): Seq[AttributeReference] = {
        exprs.map {
            case a@AttributeReference(name, dataType, _, _) => Option(a)
            case _ => None
        }.filter(_.isDefined).map(_.get)
    }

    def extractAttributeReference(expr: Expression): ArrayBuffer[AttributeReference] = {
        val columns = ArrayBuffer[AttributeReference]()
        expr transformDown {
            case a@AttributeReference(name, dataType, _, _) =>
                columns += a
                a
        }
        columns
    }

    protected def normalizePlan(plan: LogicalPlan): LogicalPlan = {
        plan transform {
            case Filter(condition: Expression, child: LogicalPlan) =>
                Filter(splitConjunctivePredicates(condition).map(rewriteEqual).sortBy(hashCode)
                  .reduce(And), child)
            case sample: Sample =>
                sample.copy(seed = 0L)
            case Join(left, right, joinType, condition, hint) if condition.isDefined =>
                val newCondition =
                    splitConjunctivePredicates(condition.get).map(rewriteEqual).sortBy(hashCode)
                      .reduce(And)
                Join(left, right, joinType, Some(newCondition), hint)
        }
    }

    private def splitConjunctivePredicates(condition: Expression): Seq[Expression] = {
        condition match {
            case And(cond1, cond2) =>
                splitConjunctivePredicates(cond1) ++ splitConjunctivePredicates(cond2)
            case other => other :: Nil
        }
    }

    private def rewriteEqual(condition: Expression): Expression = condition match {
        case eq@EqualTo(l: Expression, r: Expression) =>
            Seq(l, r).sortBy(hashCode).reduce(EqualTo)
        case eq@EqualNullSafe(l: Expression, r: Expression) =>
            Seq(l, r).sortBy(hashCode).reduce(EqualNullSafe)
        case _ => condition // Don't reorder.
    }

    private def isSPJG(plan: LogicalPlan): Boolean = {
        var isMatch = true
        plan transformDown {
            case a@SubqueryAlias(_, Project(_, _)) =>
                isMatch = false
                a
            case a@Union(_, _, _) =>
                isMatch = false
                a
        }

        if (!isMatch) {
            return false
        }

        plan match {
            case p@Project(_, Join(_, _, _, _, _)) => true
            case p@Project(_, Filter(_, Join(_, _, _, _, _))) => true
            case p@Aggregate(_, _, Filter(_, Join(_, _, _, _, _))) => true
            case p@Aggregate(_, _, Filter(_, _)) => true
            case p@Aggregate(_, _, Join(_, _, _, _, _)) => true
            case p@Aggregate(_, _, SubqueryAlias(_, LogicalRDD(_, _, _, _, _))) => true
            case p@Aggregate(_, _, SubqueryAlias(_, LogicalRelation(_, _, _, _))) => true
            case p@Aggregate(_, _, SubqueryAlias(_, SubqueryAlias(_, LogicalRelation(_, _, _, _)))) => true
            case p@Project(_, SubqueryAlias(_, LogicalRDD(_, _, _, _, _))) => true
            case p@Project(_, SubqueryAlias(_, LogicalRelation(_, _, _, _))) => true
            case p@Project(_, Filter(_, Aggregate(_, _, _))) => false // for having
            case _ => false
        }
    }

    def hashCode(_ar: Expression): Int = {
        // See http://stackoverflow.com/questions/113511/hash-code-implementation
        _ar match {
            case ar@AttributeReference(_, _, _, _) =>
                var h = 17
                h = h * 37 + ar.name.hashCode()
                h = h * 37 + ar.dataType.hashCode()
                h = h * 37 + ar.nullable.hashCode()
                h = h * 37 + ar.metadata.hashCode()
                h = h * 37 + ar.exprId.hashCode()
                h
            case _ => _ar.hashCode()
        }
    }
}

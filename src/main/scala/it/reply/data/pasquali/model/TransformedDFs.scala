package it.reply.data.pasquali.model

import org.apache.spark.sql.DataFrame

case class TransformedDFs(toHive : DataFrame, toKudu : DataFrame) {
}

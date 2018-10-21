package xiatian.octopus.storage.task

import java.io.File

import org.dizitart.no2.filters.Filters
import org.dizitart.no2.{Document, Nitrite, NitriteCollection, WriteResult}

private[task] class NitriteDb(dbFile: File) {

  dbFile.getParentFile.mkdirs()

  //java initialization
  val db: Nitrite = Nitrite.builder()
    .compressed()
    .filePath(dbFile)
    .openOrCreate("xiatian", "password");

  protected def put(collection: NitriteCollection,
                    id: String,
                    doc: Document): WriteResult = {
    val filter = Filters.eq("id", id)
    val cursor = collection.find(filter)
    if (cursor.size() > 0) {
      collection.update(filter, doc)
    } else {
      collection.insert(doc)
    }
  }

}

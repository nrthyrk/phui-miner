package im.yanchen.pupgrowth

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

class ARegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Transaction])
    kryo.register(classOf[UtilList])
    kryo.register(classOf[UtilityList])
  }
}
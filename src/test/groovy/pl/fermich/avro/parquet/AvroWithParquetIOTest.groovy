package pl.fermich.avro.parquet

import org.apache.hadoop.fs.Path
import pl.fermich.avro.model.User
import spock.lang.Specification

class AvroWithParquetIOTest extends Specification {

    def 'should serialize and serialize parquet with avro file'() {
        given:
        def avroIo = new AvroWithParquetIO()
        def avroFile = new File('/tmp/users-parquet-file.avro')
        avroFile.delete()
        def path = new Path(avroFile.path)
        def users = [
                User.newBuilder().setLogin("user1").setName("User Name 1").setAge(100).build(),
                User.newBuilder().setLogin("user2").setName("User Name 2").setAge(50).build(),
                User.newBuilder().setLogin("user3").setName("User Name 3").setAge(25).build()
        ]

        when:
        avroIo.write(path, users)

        then:
        avroIo.read(path).each {
            assert users.contains(it)
        }
    }

}

package pl.fermich.avro.trevni

import pl.fermich.avro.model.User
import spock.lang.Specification

class AvroWithTrevniIOTest extends Specification {
    def 'should serialize and deserialize users using avros builtin column storage format Trevni'() {
        given:
        def avroIo = new AvroWithTrevniIO()
        def avroFile = File.createTempFile("users-trevni-file", ".avro")
        def users = [
                User.newBuilder().setLogin("user1").setName("User Name 1").setAge(100).build(),
                User.newBuilder().setLogin("user2").setName("User Name 2").setAge(50).build(),
                User.newBuilder().setLogin("user3").setName("User Name 3").setAge(25).build()
        ]

        when:
        avroIo.write(avroFile, users)

        then:
        avroIo.read(avroFile).each {
            assert users.contains(it)
        }
    }
}

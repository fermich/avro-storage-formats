package pl.fermich.avro.seqfile

import pl.fermich.avro.model.User

class AvroSequenceFileIOTest extends spock.lang.Specification {

    def 'should serialize and deserialize avro using row oriented sequence file'() {
        given:
        def avroIo = new AvroSequenceFileIO()
        def avroFile = File.createTempFile("users-sequence-file", ".avro")
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

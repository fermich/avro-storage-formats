package pl.fermich.avro.trevni;

import com.google.common.collect.Lists;
import org.apache.avro.generic.GenericData.Record;
import org.apache.trevni.ColumnFileMetaData;
import org.apache.trevni.avro.AvroColumnReader;
import org.apache.trevni.avro.AvroColumnReader.Params;
import org.apache.trevni.avro.AvroColumnWriter;
import pl.fermich.avro.model.User;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class AvroWithTrevniIO {
    public void write(File file, List<User> users) throws IOException {
        AvroColumnWriter<User> writer = new AvroColumnWriter<>(User.SCHEMA$, new ColumnFileMetaData());
        for (User user : users) {
            writer.write(user);
        }
        writer.writeTo(file);
    }

    public List<User> read(File file) throws IOException {
        AvroColumnReader<Record> reader = new AvroColumnReader<>(new Params(file));

        List<User> users = Lists.newArrayList();
        while (reader.hasNext()) {
            Record userRecord = reader.next();
            User user = new User().newBuilder()
                    .setLogin(String.valueOf(userRecord.get("login")))
                    .setName(String.valueOf(userRecord.get("name")))
                    .setAge(Integer.valueOf(String.valueOf(userRecord.get("age")))).build();
            users.add(user);
        }
        return users;
    }
}

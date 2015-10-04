package pl.fermich.avro.seqfile;

import com.google.common.collect.Lists;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import pl.fermich.avro.model.User;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class AvroSequenceFileIO {

    public void write(File file, List<User> users) throws IOException {
        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<>(User.class);
        DataFileWriter<User> dataFileWriter = new DataFileWriter<>(userDatumWriter);
        dataFileWriter.create(User.SCHEMA$, file);
        for (User user : users) {
            dataFileWriter.append(user);
        }

        dataFileWriter.close();
    }

    public List<User> read(File file) throws IOException {
        DatumReader<User> userDatumReader = new SpecificDatumReader<>(User.class);
        DataFileReader<User> dataFileReader = new DataFileReader<>(file, userDatumReader);
        User user = null;
        List<User> users = Lists.newArrayList();
        while (dataFileReader.hasNext()) {
            user = dataFileReader.next(user);
            users.add(user);
        }
        return users;
    }
}

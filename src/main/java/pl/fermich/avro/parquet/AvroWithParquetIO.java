package pl.fermich.avro.parquet;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import parquet.avro.AvroParquetReader;
import parquet.avro.AvroParquetWriter;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;
import pl.fermich.avro.model.User;

import java.io.IOException;
import java.util.List;

public class AvroWithParquetIO {

    public void write(Path path, List<User> users) throws IOException {
        AvroParquetWriter<User> writer = new AvroParquetWriter<>(
                path,
                User.SCHEMA$,
                CompressionCodecName.SNAPPY,
                ParquetWriter.DEFAULT_BLOCK_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                true);

        for (User user: users) {
            writer.write(user);
        }

        writer.close();
    }

    public List<User> read(Path file) throws IOException {
        ParquetReader<User> reader = AvroParquetReader.<User>builder(file).build();
        List<User> users = Lists.newArrayList();
        User user;
        while ((user = reader.read()) != null) {
            users.add(user);
        }
        reader.close();

        return users;
    }
}

package simulator;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import com.solacesystems.jcsmp.JCSMPException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import payment.Debit;


public class AvroHelper {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    public AvroHelper(){


    }

    public byte[] serialize(Debit debit) throws Exception{
        DatumWriter<Debit> writer = new SpecificDatumWriter<>(Debit.class);
        baos.reset();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
        writer.write(debit, encoder);
        encoder.flush();
        baos.flush();
        byte[] mybytes = baos.toByteArray();
        return(mybytes);
    }

    public Debit deserialize(byte[] bytes) throws Exception{
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        DatumReader<Debit> reader = new SpecificDatumReader<>(Debit.class);
        Object obj = reader.read(null, decoder);
        // System.out.println(obj);
        return (Debit)obj;
    }
}


package learning.spark.utils;

import javax.persistence.AttributeConverter;
import java.nio.ByteBuffer;
import java.util.UUID;

public class UuidConverter implements AttributeConverter<UUID, byte[]>
{
    public UuidConverter()
    {
    }

    private long toLong(byte[] bytes, int offset, int length)
    {
        if (length == 8 && offset + length <= bytes.length)
        {
            long l = 0L;

            for (int i = offset; i < offset + length; ++i)
            {
                l <<= 8;
                l ^= (long) (bytes[i] & 255);
            }

            return l;
        }
        else
        {
            return 0L;
        }
    }

    @Override
    public byte[] convertToDatabaseColumn(UUID uuid)
    {
        if (uuid == null)
        {
            return null;
        }
        else
        {
            ByteBuffer buffer = ByteBuffer.allocate(16);
            buffer.putLong(uuid.getMostSignificantBits());
            buffer.putLong(uuid.getLeastSignificantBits());
            return buffer.array();
        }
    }

    @Override
    public UUID convertToEntityAttribute(byte[] dbData)
    {
        if (dbData != null && dbData.length == 16)
        {
            long most = this.toLong(dbData, 0, 8);
            long least = this.toLong(dbData, 8, 8);
            return new UUID(most, least);
        }
        else
        {
            return null;
        }
    }
}

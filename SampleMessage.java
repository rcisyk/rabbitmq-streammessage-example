import java.util.Arrays;
import javax.jms.StreamMessage;
import javax.jms.JMSException;

public class SampleMessage
{
    private String _messageName;
    private long _messageNum;
    private String _regionName;
    private SampleComponent[] _components;

    private static class SampleComponent
    {
        private int _key1;
        private int _key2;
        private double _value;

        SampleComponent()
        {}

        SampleComponent(int key1, int key2, double value)
        {
            _key1 = key1;
            _key2 = key2;
            _value = value;
        }

        void convertToStreamMessage(StreamMessage sm)
            throws JMSException
        {
            sm.writeInt(_key1);
            sm.writeInt(_key2);
            sm.writeDouble(_value);
        }

        void convertFromStreamMessage(StreamMessage sm)
            throws JMSException
        {
            _key1 = sm.readInt();
            _key2 = sm.readInt();
            _value = sm.readDouble();
        }

        public String toString()
        {
            return new StringBuilder("SampleComponent[")
                .append("_key1=").append(_key1)
                .append(", _key2=").append(_key2)
                .append(", _value=").append(_value)
                .append("]").toString();
        }
    }

    public SampleMessage()
    {}

    public SampleMessage(String messageName, long messageNum,
        String regionName, int numComponents)
    {
        _messageName = messageName;
        _messageNum = messageNum;
        _regionName = regionName;

        _components = new SampleComponent[numComponents];
        for (int i = 0; i < numComponents; i++)
            _components[i] = new SampleComponent(i, 2* i,
                i + 0.7549443298316844);
    }

    public void convertToStreamMessage(StreamMessage sm)
        throws JMSException
    {
        sm.writeString(_messageName);
        sm.writeLong(_messageNum);
        sm.writeInt(_components.length);
        for (int i = 0; i < _components.length; i++) {
            sm.writeInt(i);
            _components[i].convertToStreamMessage(sm);
        }
        sm.writeString(_regionName);
    }

    public void convertFromStreamMessage(StreamMessage sm)
        throws JMSException
    {
        _messageName = sm.readString();
        _messageNum = sm.readLong();

        int numOfComponents = sm.readInt();
        _components = new SampleComponent[numOfComponents];

        for (int i = 0; i < numOfComponents; i++) {
            System.out.println("Processing component " + sm.readInt());
            _components[i] = new SampleComponent();
            _components[i].convertFromStreamMessage(sm);
        }

        _regionName = sm.readString();
    }

    public String toString()
    {
        return new StringBuilder("SampleMessage[")
            .append("_messageName=").append(_messageName)
            .append(", _messageNum=").append(_messageNum)
            .append(", _regionName=").append(_regionName)
            .append(", _components=").append(Arrays.toString(_components))
            .append("]").toString();
    }
}

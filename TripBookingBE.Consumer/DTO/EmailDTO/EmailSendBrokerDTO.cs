namespace TripBookingBE.Consumer.DTO.EmailDTO;

public class EmailSendBrokerDTO
{
    public string ToEmail {get;set;} = string.Empty;
    public string Subject{get;set;} = string.Empty;
    public string Htmlbody{get;set;} = string.Empty;
    public string PlainText{get;set;} = string.Empty;
}
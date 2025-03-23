use std::sync::{
    atomic::{AtomicI32, Ordering},
    Arc,
};

use log::{debug, error, info, warn};
use time::{macros::format_description, OffsetDateTime};
use time_tz::{timezones, OffsetResult, PrimitiveDateTimeExt, Tz};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    sync::Mutex,
};

use crate::{
    messages::{IncomingMessages, OutgoingMessages, RequestMessage, ResponseMessage},
    server_versions, Error,
};

pub struct Client {
    pub(crate) server_version: i32,
    pub(crate) connection_time: Option<OffsetDateTime>,
    pub(crate) time_zone: Option<&'static Tz>,
    pub(crate) message_bus: Arc<MessageBus>,

    client_id: i32,             // ID of client.
    next_request_id: AtomicI32, // Next available request_id.
    order_id: AtomicI32,        // Next available order_id. Starts with value returned on connection.
}

impl Client {
    pub async fn connect(address: &str, client_id: i32) -> Result<Self, Error> {
        let connection = Connection::connect(client_id, address).await?;
        let connection_metadata = connection.connection_metadata.lock().await;

        let message_bus = Arc::new(MessageBus {});

        // message_bus.process_messages(connection_metadata.server_version)?;

        let client = Client {
            server_version: connection_metadata.server_version,
            connection_time: connection_metadata.connection_time,
            time_zone: connection_metadata.time_zone,
            message_bus,
            client_id: connection_metadata.client_id,
            next_request_id: AtomicI32::new(9000),
            order_id: AtomicI32::new(connection_metadata.next_order_id),
        };

        Ok(client)
    }

    pub fn client_id(&self) -> i32 {
        self.client_id
    }

    pub fn next_request_id(&self) -> i32 {
        self.next_request_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn next_order_id(&self) -> i32 {
        self.order_id.fetch_add(1, Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_client_id() {
        let client = Client::connect("127.0.0.1:7497", 2001).await.unwrap();
        println!("client_id: {:?}", client.client_id());
        println!("next_request_id: {:?}", client.next_request_id());
        println!("next_order_id: {:?}", client.next_order_id());
    }
}

struct MessageBus {}

impl MessageBus {
    // Sends formatted message to TWS and creates a reply channel by request id.
    async fn send_request(&self, request_id: i32, packet: &RequestMessage) -> Result<(), Error> {
        todo!()
    }

    // Sends formatted message to TWS and creates a reply channel by request id.
    async fn cancel_subscription(&self, request_id: i32, packet: &RequestMessage) -> Result<(), Error> {
        todo!()
    }

    // Sends formatted message to TWS and creates a reply channel by message type.
    async fn send_shared_request(&self, message_id: OutgoingMessages, packet: &RequestMessage) -> Result<(), Error> {
        todo!()
    }

    // Sends formatted message to TWS and creates a reply channel by message type.
    async fn cancel_shared_subscription(&self, message_id: OutgoingMessages, packet: &RequestMessage) -> Result<(), Error> {
        todo!()
    }

    // Sends formatted order specific message to TWS and creates a reply channel by order id.
    async fn send_order_request(&self, request_id: i32, packet: &RequestMessage) -> Result<(), Error> {
        todo!()
    }

    async fn cancel_order_subscription(&self, request_id: i32, packet: &RequestMessage) -> Result<(), Error> {
        todo!()
    }

    async fn ensure_shutdown(&self) {
        todo!()
    }

    // Testing interface. Tracks requests sent messages when Bus is stubbed.
    #[cfg(test)]
    async fn request_messages(&self) -> Vec<RequestMessage> {
        vec![]
    }
}

pub(crate) type Response = Result<ResponseMessage, Error>;

#[derive(Debug)]
struct Connection {
    client_id: i32,
    connection_url: String,
    reader: Mutex<OwnedReadHalf>,
    writer: Mutex<OwnedWriteHalf>,
    connection_metadata: Mutex<ConnectionMetadata>,
    max_retries: i32,
}
const MIN_SERVER_VERSION: i32 = 100;
const MAX_SERVER_VERSION: i32 = server_versions::WSH_EVENT_DATA_FILTERS_DATE;
const MAX_RETRIES: i32 = 20;

impl Connection {
    pub async fn connect(client_id: i32, connection_url: &str) -> Result<Self, Error> {
        let tcp_stream = TcpStream::connect(connection_url).await?;
        let (reader, writer) = tcp_stream.into_split();

        let connection = Self {
            client_id,
            connection_url: connection_url.into(),
            reader: Mutex::new(reader),
            writer: Mutex::new(writer),
            connection_metadata: Mutex::new(ConnectionMetadata {
                client_id,
                ..Default::default()
            }),
            max_retries: MAX_RETRIES,
        };

        connection.establish_connection().await?;

        Ok(connection)
    }

    async fn establish_connection(&self) -> Result<(), Error> {
        self.handshake().await?;
        self.start_api().await?;
        self.receive_account_info().await?;
        Ok(())
    }

    async fn write(&self, data: &str) -> Result<(), Error> {
        let mut writer = self.writer.lock().await;
        writer.write_all(data.as_bytes()).await?;
        Ok(())
    }

    async fn write_message(&self, message: &RequestMessage) -> Result<(), Error> {
        let mut writer = self.writer.lock().await;

        let data = message.encode();
        debug!("-> {data:?}");

        let packet = buffer_helper::encode_packet_raw(&data);

        writer.write_all(&packet).await?;

        // self.recorder.record_request(message);

        Ok(())
    }

    async fn read_message(&self) -> Response {
        let mut reader = self.reader.lock().await;

        let message_size = read_header(&mut reader).await?;
        let mut data = vec![0_u8; message_size];

        reader.read_exact(&mut data).await?;

        let raw_string = String::from_utf8(data)?;
        debug!("<- {:?}", raw_string);

        let message = ResponseMessage::from(&raw_string);
        // self.recorder.record_response(&message);

        Ok(message)
    }

    async fn handshake(&self) -> Result<(), Error> {
        let prefix = "API\0";
        let version = format!("v{MIN_SERVER_VERSION}..{MAX_SERVER_VERSION}");

        let packet = prefix.to_owned() + &buffer_helper::encode_packet(&version);
        self.write(&packet).await?;

        let ack = self.read_message().await;

        let mut connection_metadata = self.connection_metadata.lock().await;

        match ack {
            Ok(mut response) => {
                connection_metadata.server_version = response.next_int()?;

                let time = response.next_string()?;
                (connection_metadata.connection_time, connection_metadata.time_zone) = parse_connection_time(time.as_str());
            }
            Err(Error::Io(err)) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Err(Error::Simple(format!("The server may be rejecting connections from this host: {err}")));
            }
            Err(err) => {
                return Err(err);
            }
        }
        Ok(())
    }

    async fn start_api(&self) -> Result<(), Error> {
        const VERSION: i32 = 2;

        let prelude = &mut RequestMessage::default();

        prelude.push_field(&OutgoingMessages::StartApi);
        prelude.push_field(&VERSION);
        prelude.push_field(&self.client_id);

        if self.server_version().await > server_versions::OPTIONAL_CAPABILITIES {
            prelude.push_field(&"");
        }

        self.write_message(prelude).await?;

        Ok(())
    }

    async fn server_version(&self) -> i32 {
        let connection_metadata = self.connection_metadata.lock().await;
        connection_metadata.server_version
    }

    async fn receive_account_info(&self) -> Result<(), Error> {
        let mut saw_next_order_id: bool = false;
        let mut saw_managed_accounts: bool = false;

        let mut attempts = 0;
        const MAX_ATTEMPTS: i32 = 100;
        loop {
            let mut message = self.read_message().await?;

            match message.message_type() {
                IncomingMessages::NextValidId => {
                    saw_next_order_id = true;

                    message.skip(); // message type
                    message.skip(); // message version

                    let mut connection_metadata = self.connection_metadata.lock().await;
                    connection_metadata.next_order_id = message.next_int()?;
                }
                IncomingMessages::ManagedAccounts => {
                    saw_managed_accounts = true;

                    message.skip(); // message type
                    message.skip(); // message version

                    let mut connection_metadata = self.connection_metadata.lock().await;
                    connection_metadata.managed_accounts = message.next_string()?;
                }
                IncomingMessages::Error => {
                    error!("message: {message:?}")
                }
                _ => info!("message: {message:?}"),
            }

            attempts += 1;
            if (saw_next_order_id && saw_managed_accounts) || attempts > MAX_ATTEMPTS {
                break;
            }
        }

        Ok(())
    }
}

#[derive(Default, Clone, Debug)]
pub(crate) struct ConnectionMetadata {
    pub(crate) next_order_id: i32,
    pub(crate) client_id: i32,
    pub(crate) server_version: i32,
    pub(crate) managed_accounts: String,
    pub(crate) connection_time: Option<OffsetDateTime>,
    pub(crate) time_zone: Option<&'static Tz>,
}

async fn read_header(reader: &mut OwnedReadHalf) -> Result<usize, Error> {
    let mut buffer = [0; 4];
    reader.read_exact(&mut buffer).await?;

    buffer_helper::read_header_buffer(&buffer)
}

fn parse_connection_time(connection_time: &str) -> (Option<OffsetDateTime>, Option<&'static Tz>) {
    let parts: Vec<&str> = connection_time.split(' ').collect();

    let zones = timezones::find_by_name(parts[2]);
    if zones.is_empty() {
        error!("time zone not found for {}", parts[2]);
        return (None, None);
    }

    let timezone = zones[0];

    let format = format_description!("[year][month][day] [hour]:[minute]:[second]");
    let date_str = format!("{} {}", parts[0], parts[1]);
    let date = time::PrimitiveDateTime::parse(date_str.as_str(), format);
    match date {
        Ok(connected_at) => match connected_at.assume_timezone(timezone) {
            OffsetResult::Some(date) => (Some(date), Some(timezone)),
            _ => {
                warn!("error setting timezone");
                (None, Some(timezone))
            }
        },
        Err(err) => {
            warn!("could not parse connection time from {date_str}: {err}");
            (None, Some(timezone))
        }
    }
}

mod buffer_helper {
    use std::io::{Cursor, Write};

    use crate::Error;

    use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

    pub fn read_header_buffer(buffer: &[u8; 4]) -> Result<usize, Error> {
        let mut reader = Cursor::new(buffer);
        let count = reader.read_u32::<BigEndian>()?;

        Ok(count as usize)
    }

    pub fn encode_packet_raw(message: &str) -> Vec<u8> {
        let data = message.as_bytes();

        let mut packet: Vec<u8> = Vec::with_capacity(data.len() + 4);

        packet.write_u32::<BigEndian>(data.len() as u32).unwrap();
        packet.write_all(data).unwrap();

        packet
    }

    pub fn encode_packet(message: &str) -> String {
        let packet = encode_packet_raw(message);

        std::str::from_utf8(&packet).unwrap().into()
    }
}

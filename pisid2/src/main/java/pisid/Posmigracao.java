package pisid;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;

import static com.hivemq.client.mqtt.MqttGlobalPublishFilter.ALL;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.CharBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;




public class Posmigracao {
	
	final static String host = "9c0cafee45454ac597fe285b8bdf4b7e.s2.eu.hivemq.cloud";
    final static String username = "Quiterioi";
    final static String password = "Mqtt2022";
    static Mqtt5BlockingClient client;
    static Connection connection;
	
	static void connect(){
		try {
			connection = DriverManager.getConnection("jdbc:mariadb://localhost:3306/culturas_local?user=root&password=abc");
		} catch (SQLException e) {
			
			e.printStackTrace();
		}
		
        //create an MQTT client
        client = MqttClient.builder()
                .useMqttVersion5()
                .serverHost(host)
                .serverPort(8883)
                .sslWithDefaultConfig()
                .buildBlocking();

        //connect to HiveMQ Cloud with TLS and username/pw
        client.connectWith()
                .simpleAuth()
                .username(username)
                .password(UTF_8.encode(password))
                .applySimpleAuth()
                .send();
		
        System.out.println("Connected successfully");
		
		
	}
	
	
	static void receiveinfo(){
		
        client.subscribeWith()
                .topicFilter("PSID")
                .send();
        
        client.toAsync().publishes(ALL, publish -> {
           String arrived = (UTF_8.decode(publish.getPayload().get())).toString();
           System.out.println(arrived);
           String[] enviar = spliterdoquiterio(arrived);
           String query = "insert into medicao(Zona,Sensor,Hora,Leitura) values(?,?,?,?)";
           try {
        	
  			PreparedStatement Mypst = connection.prepareStatement(query);			
  			Mypst.setInt(1, Integer.parseInt(enviar[0]));
  			Mypst.setString(2, enviar[1]);
  			Mypst.setString(3, enviar[2]);
  			Mypst.setString(4, enviar[3]);		      
  			Mypst.execute();
  		} catch (SQLException e) {
  			
  			e.printStackTrace();
  		}
            
        });
	}
	
	
	
	
	
	
	
	private static String[] spliterdoquiterio(String arrived) {		
		 String newarrived = (arrived.replaceAll("\"", "")).replaceAll("}", "");	 
		 String[] arrived_split = newarrived.split(",");
		 String zona = arrived_split[1].split(":")[1].replaceAll(" ", "").replaceAll("Z", "");
		 String sensor = arrived_split[2].split(":")[1].replaceAll(" ", "");
		 String medicao = arrived_split[4].split(":")[1].replaceAll(" ", "");
		 String data = arrived_split[3].split(" ")[3].replaceAll("T", " ").replaceAll("L", " ").replaceAll("H", " ").replaceAll("Z","");
		 
		 String[] enviar = {zona, sensor, data, medicao};

		 return enviar;
	}
		

	public static void main(String[] args) {
		connect();
		
			receiveinfo();
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {				
				e.printStackTrace();
			
		}
		
		
	
		
	}
	
	
	

}

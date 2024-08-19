import java.io.File;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;

public class Main {

    public static void main(String[] args) {
        String url = "jdbc:postgresql://localhost:5432/postgres";
        String username = "postgres";
        String password = "111111";

        try {

            File dir = new File("src");
            File[] files = dir.listFiles((d, name) -> name.endsWith(".xml"));

            int id = 0; //переменная для добавление id в таблицу D_CAT_CATALOG

            for (File xmlFile : files) {

                //для работы с XML
                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                DocumentBuilder builder = factory.newDocumentBuilder();
                Document doc = builder.parse(xmlFile);

                id++; //при вхождение в новый файл id шагает на единицу

                // подключение к базе данных
                Class.forName("org.postgresql.Driver");
                Connection conn = DriverManager.getConnection(url, username, password);

                //новый узел, считываются данные о CATALOG
                NodeList nodeListCatalog = doc.getElementsByTagName("CATALOG");

                String uuid = "";
                String company = "";

                Node currentNode = nodeListCatalog.item(0);
                Element catalogElement = (Element) currentNode;

                uuid = catalogElement.getAttribute("uuid");
                String dateString = catalogElement.getAttribute("date");
                //парсинг date для добавления в таблицу с типом данных TimeStamp
                java.util.Date date = new SimpleDateFormat("dd.MM.yyyy").parse(dateString);

                Date dateTime = new java.sql.Date(date.getTime());

                company = catalogElement.getAttribute("company");

                // добавление uuid, date, company в d_cat_catalog
                PreparedStatement pstmtCATALOG = conn.prepareStatement("INSERT INTO d_cat_catalog (id, delivery_date, company, uuid) VALUES (?, ?, ?, ?)");
                PreparedStatement pstmtSelect = conn.prepareStatement("SELECT 1 FROM d_cat_catalog WHERE id = ? AND delivery_date = ? AND company = ? AND uuid = ?");

                pstmtSelect.setInt(1, id);
                pstmtSelect.setDate(2, dateTime);
                pstmtSelect.setString(3, company);
                pstmtSelect.setString(4, uuid);

                ResultSet rs1 = pstmtSelect.executeQuery();

                // проверка на наличие данных в таблице, чтобы не добавлять одно и то же несколько раз
                if (!rs1.next()) {
                    //добавление данных
                    pstmtCATALOG.setInt(1, id);
                    pstmtCATALOG.setDate(2, dateTime);
                    pstmtCATALOG.setString(3, company);
                    pstmtCATALOG.setString(4, uuid);
                    pstmtCATALOG.executeUpdate();
                }

                NodeList nodeList = doc.getElementsByTagName("PLANT");

                // добавление данных в f_cat_plants
                PreparedStatement pstmtPLANTS = conn.prepareStatement("INSERT INTO f_cat_plants (common, botanical, zone, light, price, availability, catalog_id) VALUES (?, ?, ?, ?, ?, ?, ?)");
                PreparedStatement pstmtSelect1 = conn.prepareStatement("SELECT 1 FROM f_cat_plants WHERE common = ? AND botanical = ? AND zone = ? AND light = ? AND price = ? AND availability = ? AND catalog_id = ?");

                // цикл по всем PLANT
                for (int i = 0; i < nodeList.getLength(); i++) {
                    Node node = nodeList.item(i);
                    Element element = (Element) node;

                    //по всем элементам внутри PLANT
                    for (int j = 0; j < node.getChildNodes().getLength(); j++) {

                        String value1 = element.getElementsByTagName("COMMON").item(0).getTextContent();
                        String value2 = element.getElementsByTagName("BOTANICAL").item(0).getTextContent();
                        String value3 = element.getElementsByTagName("ZONE").item(0).getTextContent();
                        String value4 = element.getElementsByTagName("LIGHT").item(0).getTextContent();
                        String priceStr = element.getElementsByTagName("PRICE").item(0).getTextContent();
                        Double value5 = Double.valueOf(priceStr.substring(1));
                        int value6 = Integer.parseInt(element.getElementsByTagName("AVAILABILITY").item(0).getTextContent());
                        String value7 = uuid;

                        // Set the values in the select prepared statement
                        pstmtSelect1.setString(1, value1);
                        pstmtSelect1.setString(2, value2);
                        pstmtSelect1.setString(3, value3);
                        pstmtSelect1.setString(4, value4);
                        pstmtSelect1.setDouble(5, value5);
                        pstmtSelect1.setInt(6, value6);
                        pstmtSelect1.setString(7, uuid);

                        ResultSet rs = pstmtSelect1.executeQuery();

                        if (!rs.next()) {
                            pstmtPLANTS.setString(1, value1);
                            pstmtPLANTS.setString(2, value2);
                            pstmtPLANTS.setString(3, value3);
                            pstmtPLANTS.setString(4, value4);
                            pstmtPLANTS.setDouble(5, value5);
                            pstmtPLANTS.setInt(6, value6);
                            pstmtPLANTS.setString(7, value7);
                            pstmtPLANTS.executeUpdate();
                        }
                    }
                }
                conn.close();
            }

            System.out.println("Данные успешно добавлены");
        } catch (SQLException e) {
            System.out.println("Error loading data: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("Error parsing XML file: " + e.getMessage());
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }
}
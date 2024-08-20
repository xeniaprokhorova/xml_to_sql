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

    private static void insertCatalogData(PreparedStatement preparedStatement, int id, String uuid, Date dateTime, String company) throws SQLException {
        preparedStatement.setInt(1, id);
        preparedStatement.setDate(2, dateTime);
        preparedStatement.setString(3, company);
        preparedStatement.setString(4, uuid);
    }

    private static void insertPlantData(PreparedStatement pstmtSelect, String value1, String value2, String value3, String value4, Double value5, int value6, String uuid) throws SQLException {
        pstmtSelect.setString(1, value1);
        pstmtSelect.setString(2, value2);
        pstmtSelect.setString(3, value3);
        pstmtSelect.setString(4, value4);
        pstmtSelect.setDouble(5, value5);
        pstmtSelect.setInt(6, value6);
        pstmtSelect.setString(7, uuid);
    }

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
                //парсинг date для добавления в таблицу с типом данных TimeStamp
                java.util.Date date = new SimpleDateFormat("dd.MM.yyyy").parse(catalogElement.getAttribute("date"));

                Date dateTime = new java.sql.Date(date.getTime());

                company = catalogElement.getAttribute("company");

                // добавление uuid, date, company в d_cat_catalog
                PreparedStatement pstmtCATALOG = conn.prepareStatement("INSERT INTO d_cat_catalog (id, delivery_date, company, uuid) VALUES (?, ?, ?, ?)");
                PreparedStatement pstmtSelect = conn.prepareStatement("SELECT 1 FROM d_cat_catalog WHERE id = ? AND delivery_date = ? AND company = ? AND uuid = ?");

                insertCatalogData(pstmtSelect, id, uuid, dateTime, company);

                ResultSet rs1 = pstmtSelect.executeQuery();

                // проверка на наличие данных в таблице, чтобы не добавлять одно и то же несколько раз
                if (!rs1.next()) {
                    //добавление данных
                    insertCatalogData(pstmtCATALOG, id, uuid, dateTime, company);
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

                        insertPlantData(pstmtSelect1, value1, value2, value3, value4, value5, value6, uuid);

                        ResultSet rs = pstmtSelect1.executeQuery();

                        if (!rs.next()) {
                            insertPlantData(pstmtPLANTS, value1, value2, value3, value4, value5, value6, uuid);
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
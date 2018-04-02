import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import com.opencsv.CSVReader;

public class ProcessCsv {
	public static void generateDistricts() throws IOException {
		CSVReader reader = new CSVReader(new FileReader("C:\\Users\\Programar\\Desktop\\Máster\\Ingeniería y Ciencia de Datos II\\Crimes.csv"));
		String [] line;
		reader.readNext(); //Header
		FileWriter fileWriter = new FileWriter(new File("Districts.csv"));
        PrintWriter pw = new PrintWriter(fileWriter);
        pw.println("Id,District");
		int cont = 0;
		//int contador = 0;
		Set<String> districts = new HashSet<String>();
		while((line = reader.readNext()) != null) {
			//contador++;
			String district = line[6];
			if(districts.add(district) && !district.equals("")) {
				cont++;
				pw.println(cont+","+district);
				//System.out.println(contador);
			}
		}
		pw.close();
	}
	
	public static void generateCategories() throws IOException {
		CSVReader reader = new CSVReader(new FileReader("C:\\Users\\Programar\\Desktop\\Máster\\Ingeniería y Ciencia de Datos II\\Crimes.csv"));
		String [] line;
		reader.readNext(); //Header
		FileWriter fileWriter = new FileWriter(new File("Categories.csv"));
        PrintWriter pw = new PrintWriter(fileWriter);
        pw.println("Id,Category");
		int cont = 0;
        //int contador = 0;
		Set<String> categories = new HashSet<String>();
		while((line = reader.readNext()) != null) {
			//contador++;
			String category = line[1];
			if(categories.add(category) && !category.equals("")) {
				cont++;
				pw.println(cont+","+category);
				//System.out.println(contador);
			}
		}
		pw.close();
	}
	
	public static void generateIncidents() throws IOException {
		CSVReader reader = new CSVReader(new FileReader("C:\\Users\\Programar\\Desktop\\Máster\\Ingeniería y Ciencia de Datos II\\Crimes.csv"));
		String [] line;
		reader.readNext(); //Header
		FileWriter fileWriter = new FileWriter(new File("Incidents.csv"));
        PrintWriter pw = new PrintWriter(fileWriter);
        pw.println("Id,IncNum,DayOfWeek,Timestamp,Resolution");
		int cont = 0;
		//int contador = 0;
		Set<String> incnums = new HashSet<String>();
		while((line = reader.readNext()) != null) {
			//contador++;
			String incnum = line[0];
			String dayofweek = line[3];
	        String date = line[4];
	        String time = line[5];
	        int year = Integer.parseInt(date.substring(6,10));
	        int month = Integer.parseInt(date.substring(0,2));
	        int day = Integer.parseInt(date.substring(3,5));
	        int hour = Integer.parseInt(time.substring(0,2));
	        int minute = Integer.parseInt(time.substring(3));
	        Timestamp timestamp = new Timestamp(year-1900, month-1, day, hour, minute, 0, 0);
	        long tmp = timestamp.getTime();
	        String resolution = line[7];
			if(incnums.add(incnum) && !incnum.equals("")) {
				cont++;
				pw.println(cont+","+incnum+","+dayofweek+","+tmp+","+resolution);
			}
		}
		pw.close();
	}
	
	public static void generateIncToDistrict() throws IOException {
		CSVReader reader = new CSVReader(new FileReader("C:\\Users\\Programar\\Desktop\\Máster\\Ingeniería y Ciencia de Datos II\\Crimes.csv"));
		String [] line;
		reader.readNext(); //Header
		FileWriter fileWriter = new FileWriter(new File("IncToDistrict.csv"));
        PrintWriter pw = new PrintWriter(fileWriter);
        pw.println("IncNum,District");
        Set<String> incnums = new HashSet<String>();
        while((line = reader.readNext()) != null) {
			//contador++;
			String incnum = line[0];
	        String district = line[6];
			if(incnums.add(incnum) && !incnum.equals("")) {
				pw.println(incnum+","+district);
			}
		}
		pw.close();
	}
	
	public static void generateIncToCategory() throws IOException {
		CSVReader reader = new CSVReader(new FileReader("C:\\Users\\Programar\\Desktop\\Máster\\Ingeniería y Ciencia de Datos II\\Crimes.csv"));
		String [] line;
		reader.readNext(); //Header
		FileWriter fileWriter = new FileWriter(new File("IncToCategory.csv"));
        PrintWriter pw = new PrintWriter(fileWriter);
        pw.println("IncNum,Category");
        while((line = reader.readNext()) != null) {
			//contador++;
			String incnum = line[0];
	        String category = line[1];
			pw.println(incnum+","+category);

		}
		pw.close();
	}
	
	public static void main(String[] args) throws IOException {
		generateDistricts();
		System.out.println("Districts.csv generated");
		generateCategories();
		System.out.println("Categories.csv generated");
		generateIncidents();
		System.out.println("Incidents.csv generated");
		generateIncToDistrict();
		System.out.println("IncToDistrict.csv generated");
		generateIncToCategory();
		System.out.println("IncToCategory.csv generated");
	}
}

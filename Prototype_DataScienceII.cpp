//============================================================================
// Name        : Prototype_DataScienceII.cpp
// Author      : Grupo 6
// Version     : 1.0
// Copyright   : Your copyright notice
// Description : C++ Prototype for Data Science II project
//============================================================================

#include <iostream>
#include <fstream>
#include <string>

using namespace std;

int main() {
	//int aux = 0;
	int vandalism = 0;
	int meth = 0;
	int domestic = 0;
	int franklin = 0;
	string line;
	string id, category, descript, dayofweek, date, time, district, resolution, address, x, y, location;
	cout << "C++ PROTOTYPE\n" << endl;
	ifstream file;
	file.open ("Crimes.csv"); //Replace this line
	//file.open("path/to/Crimes.csv"); Use this one
	if(file.is_open()){
		getline(file, line); //HEADER
		while (std::getline(file, line)) {
		/*while(aux<20){
			aux++;
			std::getline(file,line);*/
			const char *mystart=line.c_str();
			bool instring = false;
			int cont = 0;
			for (const char* p=mystart; *p; p++) {
				if (*p=='"')
					instring = !instring;
				else if (*p==',' && !instring) {
					switch(cont){
						case 0:
							id = string(mystart,p-mystart);
							break;
						case 1:
							category = string(mystart,p-mystart);
							break;
						case 2:
							descript = string(mystart,p-mystart);
							break;
						case 3:
							dayofweek = string(mystart,p-mystart);
							break;
						case 4:
							date = string(mystart,p-mystart);
							break;
						case 5:
							time = string(mystart,p-mystart);
							break;
						case 6:
							district = string(mystart,p-mystart);
							break;
						case 7:
							resolution = string(mystart,p-mystart);
							break;
						case 8:
							address = string(mystart,p-mystart);
							break;
						case 9:
							x = string(mystart,p-mystart);
							break;
						case 10:
							y = string(mystart,p-mystart);
							break;
						default:
							break;
					}
					mystart=p+1;
					cont++;
				}
			}
			location = string(mystart);
			//Vandalism case
			if (category.compare(string("VANDALISM"))==0){
				vandalism++;
			}
			//Meth possession case on weekend
			if(descript.compare(string("POSSESSION OF METH-AMPHETAMINE"))==0){
				if(dayofweek.compare(string("Saturday"))==0 || dayofweek.compare(string("Sunday"))==0){
					meth++;
				}
			}
			//Domestic violence case not resolved
			if(descript.compare(string("DOMESTIC VIOLENCE"))==0){
				if(resolution.compare(string("NONE"))==0){
					domestic++;
				}
			}
			//Stolen automovile in Franklin St / Turk St
			if(descript.compare(string("STOLEN AUTOMOBILE"))==0){
				if(address.compare(string("FRANKLIN ST / TURK ST"))==0){
					franklin++;
				}
			}
		}
		file.close();
	}else{
		cout << "Unable to open file";
	}

	cout << "Vandalism cases: " << vandalism << endl;
	cout << "Meth possession cases on weekends: " << meth << endl;
	cout << "Domestic violence cases not resolved: " << domestic << endl;
	cout << "Stolen automobiles in Franklin St / Turk St: " << franklin << endl;
	return 0;
}

import json
import datetime, time
from urllib.parse import urlencode
import requests
import uuid
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session
from sqlalchemy import create_engine, and_
from sqlalchemy import Column, String, Integer, Date, DateTime, Time
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.schema import ForeignKey, UniqueConstraint
from sqlalchemy.sql.sqltypes import PickleType
import zeep




    
    
def getToken():
    print('start gettign token')
    
    try:

        client_id=''
        client_secret=''
        url=''
        
        data = {'grant_type': 'client_credentials'}

        access_token_response = requests.post(url, data=data, verify=False, allow_redirects=False, auth=(client_id, client_secret))
        
        if access_token_response.status_code !=200:
            print("Failed to obtain token from the OAuth")
            
        token = json.loads(access_token_response.text)
        print(token)
        return token['access_token']

    except Exception as er:
        print(er)


def timeFormat(data,time):
    if time != '' and time != ' ' and time != None:
        dateTime = datetime.datetime.combine(datetime.datetime.strptime(data, "%d/%m/%Y").date(),
                                                                    datetime.datetime.strptime(time, "%H:%M").time())
        return dateTime.strftime('%Y-%m-%dT%H:%M:00')
    else:
        return None
   
def dateFormat(originDate):
    if originDate != '' and originDate != ' ' and originDate != None:
        origin = datetime.datetime.strptime(originDate, "%d/%m/%Y").date()
        return origin.strftime('%Y-%m-%d')
    else:
        return None
        
def convertValueStr(value):
    if value != '' and value != ' ' and value != None or isinstance(value, int):
        return str(value)
    else:
        return None



        
def sendData(soapFlight, uu_id):
    

    
    # Note keep both, arr and dep RUH

    # logic --- send only update ---->> flight even if the flight already been sent....
    
    try:

        
        token = getToken()
    
        
        if token != None:
            print(token)
            
            
            url = ''
    
            header = {
                  "Authorization": f"bearer {token}",
                  "transaction-id": f"{uu_id}",
                  "Originator": "",
                  "Content-type": "application/json",
                  "Accept": "application/json"
                }
          
            
          
          
            listOfSaudiAirports = ['AHB','HOF','ULH','ELQ', 'WAE', 'DHA', 'TBD','KMX', 'DMM','JED','MED','RUH','TIF','YNB','HAS','GIZ','TUU','ABT','AJF','EJH','RAE','BHH','DWD','URY','QJB','KMC','EAM','NUM','AQI','RAH','SHW','TUI','WAE']
            
            if soapFlight.FlightDep in listOfSaudiAirports:
                internationalStatus = "Domestic"
            else:
                internationalStatus = "International"
                
            
            
            


            data = {
                "internationalStatus":internationalStatus,
                "flightNumber":convertValueStr(soapFlight.FlightNo),
                "departureAirport":convertValueStr(soapFlight.FlightDep),
                "arrivalAirport":convertValueStr(soapFlight.FlightArr),
                "originDate": dateFormat(soapFlight.FlightDate),
                "scheduledDeparture":timeFormat(soapFlight.FlightDate, soapFlight.FlightStd),
                "estimatedDeparture":timeFormat(soapFlight.FlightDate, soapFlight.FlightEtd),
                "scheduledArrival":timeFormat(soapFlight.FlightDate, soapFlight.FLightSta),
                "estimatedArrival":timeFormat(soapFlight.FlightDate, soapFlight.FlightEta),
                "airlineCode":convertValueStr(soapFlight.FlightCarrier),
                "aircraftSubType":convertValueStr(soapFlight.FlightAcType),
                "registration":convertValueStr(soapFlight.FlightReg),
                "serviceType": 'J',
                "numberOfPassengers": convertValueStr(soapFlight.FlightNoOfPax)
                }
            
                

            data = json.dumps(data)
            
            res = requests.post(url, data=data, headers=header)
            
            if res.status_code == 401:
                print(res)
            else:
                print(res)
    except Exception as er:
        print(er)
        



def convertStrToTime(string):
    if string != '' and string != ' ' and string != None:
        return datetime.datetime.strptime(
            string, "%H:%M").time()
    else:
        return None


def connectionDB():
    

    db_string = ''

    engine = create_engine(db_string)

    
    base = declarative_base()

    class RAC_Flight(base):
        
        __tablename__ = 'RAC_Flight_Data_Transfer'
        
        id = Column(Integer, primary_key=True, autoincrement=True)
        cdt = Column(DateTime, default=datetime.datetime.utcnow())
    

        flightDate =  Column(DateTime, nullable=False)
        flightNo = Column(String(4), nullable=False)
        flightCarrier =Column(String(2), nullable=True)
        reg = Column(String(6), nullable=True)
        dep = Column(String(3), nullable=True)
        arr = Column(String(3), nullable=True)
        std = Column(Time, nullable=True)
        sta = Column(Time, nullable=True)
        etd = Column(Time, nullable=True)
        eta = Column(Time, nullable=True)
        acType =Column(String, nullable=True)
        fType = Column(String, nullable=True)
        NoOfPax = Column(String, nullable=True)
        transactionID = Column(String, nullable=True)
    
        __table_args__ = (UniqueConstraint('flightDate', 'flightNo', 'NoOfPax', 'eta', 'etd', name='flightData_flightNo_NoOfPax_eta_etd'), )
        
    
    
    if not engine.dialect.has_table(engine, 'RAC_Flight', schema=None):
        base.metadata.create_all(engine)


    Session = sessionmaker(engine)  
    session = Session()
    print('all running')
    return [RAC_Flight, session]


def flightDetails():
    
    try:

        startDate = datetime.datetime.now() + datetime.timedelta(days=-2)
        endDate = datetime.datetime.now() + datetime.timedelta(days=2)
        print(startDate)
        print(endDate)
        req = zeep.Client(
                wsdl="")
            
        data_se = {
                'UN': '1',
                'PSW': '1111',
                'FromDD': str(startDate.day),
                'FromMMonth': str(startDate.month),
                'FromYYYY': str(startDate.year),
                'FromHH': '',
                'FromMMin': '',
                'ToDD': str(endDate.day),
                'ToMMonth': str(endDate.month),
                'ToYYYY': str(endDate.year),
                'ToHH': '',
                'ToMMin': '',

            }
        res = req.service.FlightDetailsForPeriod(**data_se)
        print("RES AIMS API")
        return res
    except Exception as er:
        print("ERROR AIMS API")
        print(er)


def convertStrToTime(string):
    if string != '' and string != ' ' and string != None:
        return datetime.datetime.strptime(
            string, "%H:%M").time()
    else:
        return None


def main():

    start = time.time()
    print("timer start")
    try:
        res = flightDetails()
        try:
            Flight, session = connectionDB()
            for soapFlight in res.FlightList:
                
                if soapFlight.FlightDep == 'RUH' or soapFlight.FlightArr == 'RUH':
                    uu_id = uuid.uuid4()

                    flights = session.query(Flight).filter(and_(
                        Flight.flightDate==datetime.datetime.strptime(
                            soapFlight.FlightDate, "%d/%m/%Y").date(),
                        Flight.flightNo==str(soapFlight.FlightNo), 
                        Flight.etd == convertStrToTime(soapFlight.FlightEtd), 
                        Flight.eta == convertStrToTime(soapFlight.FlightEta),
                        Flight.NoOfPax == str(soapFlight.FlightNoOfPax)
                        )).first()
                    
                    if flights == None:
                        print('adding new flight or new update')
                        fly = Flight(
                            flightDate=datetime.datetime.strptime(
                                soapFlight.FlightDate, "%d/%m/%Y").date(),
                            flightNo=soapFlight.FlightNo,
                            flightCarrier=soapFlight.FlightCarrier,
                            reg=soapFlight.FlightReg,
                            dep=soapFlight.FlightDep,
                            arr=soapFlight.FlightArr,
                            std=convertStrToTime(soapFlight.FlightStd),
                            sta=convertStrToTime(soapFlight.FLightSta),
                            etd=convertStrToTime(soapFlight.FlightEtd),
                            eta=convertStrToTime(soapFlight.FlightEta),
                            acType=soapFlight.FlightAcType,
                            fType=soapFlight.FlightType,
                            NoOfPax=soapFlight.FlightNoOfPax,
                            transactionID = str(uu_id)
                        )
                        session.add(fly)
                        sendData(soapFlight, uu_id)
                        
                        
                    else:
                        print('no update or new')
            session.commit()
            session.close()
            print('saved and closed')
            
        except Exception as er:
            print(er)
            print('model and seesion issues')


    except Exception as er:
        print('no aims data')
    end = time.time()
    print(end - start)
    print("timer ended")


    
main()

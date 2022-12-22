from neo4j import GraphDatabase
import pandas as pd
from pandas import DataFrame

class Neo4jConnection:

    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
        except Exception as e:
            print("Failed to create the driver:", e)

    def close(self):
        if self.__driver is not None:
            self.__driver.close()

    def query(self, query, db=None):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try:
            session = self.__driver.session(database=db) if db is not None else self.__driver.session()
            response = list(session.run(query))
        except Exception as e:
            print("Query failed:", e)
        finally:
            if session is not None:
                session.close()
        return response
conn = Neo4jConnection(uri="bolt://localhost:11004", user="user", pwd="0000")

conn.query("""MATCH (n) DETACH DELETE n""", db = 'lab4')

def CreateDB():
    query = """ CREATE 
    (b2:Bus {name: "b2", route: "Klaipeda-Siauliai-Radviliskis-Vilnius"}),
    (b3:Bus {name: "b3", route: "Klaipeda-Kaunas"}),
    (b4:Bus {name: "b4", route: "Kaunas-Vilnius"}),
    (b5:Bus {name: "b5", route: "Siauliai-Panevezys-Vilnius"}),


    (c1:City {name: "Siauliai"}),
    (c3:City {name: "Klaipeda"}),
    (c4:City {name: "Radviliskis"}),
    (c5:City {name: "Vilnius"}),
    (c6:City {name: "Kaunas"}),
    (c7:City {name: "Panevezys"}),


    (b2)-[:TRAVEL_TO]->(c3),
    (b2)-[:TRAVEL_TO]->(c1),
    (b2)-[:TRAVEL_TO]->(c4),
    (b2)-[:TRAVEL_TO]->(c5),

    (b3)-[:TRAVEL_TO]->(c3),
    (b3)-[:TRAVEL_TO]->(c5),

    (b4)-[:TRAVEL_TO]->(c6),
    (b4)-[:TRAVEL_TO]->(c5),

    (b5)-[:TRAVEL_TO]->(c1),
    (b5)-[:TRAVEL_TO]->(c7),
    (b5)-[:TRAVEL_TO]->(c5),


    (c1)-[:ROAD {cost: 5}]->(c4),
    (c1)-[:ROAD {cost: 5.5}]->(c7),
    (c1)-[:ROAD {cost: 7}]->(c3),

    (c3)-[:ROAD {cost: 7}]->(c1),
    (c3)-[:ROAD {cost: 9}]->(c6),

    (c6)-[:ROAD {cost: 7}]->(c3),
    (c6)-[:ROAD {cost: 4.5}]->(c5),

    (c5)-[:ROAD {cost: 4.5}]->(c6),
    (c5)-[:ROAD {cost: 6}]->(c7),

    (c7)-[:ROAD {cost: 5.5}]->(c1),
    (c7)-[:ROAD {cost: 6}]->(c5),

    (c4)-[:ROAD {cost: 7.5}]->(c5)
    """
    conn.query(query, db = "lab4")

def FindRoute(bus):
    findBus = (f"MATCH (b:Bus) WHERE b.name = '{bus}' RETURN b.name, b.route")
    print(conn.query(findBus, db = "lab4"))

def FindStops(stop):
    findStops = (f"MATCH stops = (b:Bus WHERE b.name = '{stop}')-[:TRAVEL_TO]-(city) RETURN city.name as cityName")
    print(conn.query(findStops, db = "lab4"))

def FindAllRoutes(a, b):
    findAllRoutes = (f"MATCH p = (a:City WHERE a.name = '{a}')-[:ROAD*]-(b:City WHERE b.name = '{b}') unwind nodes(p) as node with p, collect(node.name) as roads RETURN Distinct roads")
    dtf_data = DataFrame([dict(_) for _ in conn.query(findAllRoutes, db = "lab4")])
    print(dtf_data["roads"].values)

def FindCheapestTrip(a, b):
    findCheapestTrip =(f"MATCH p = (a:City WHERE a.name = '{a}')-[:ROAD*]-(b:City WHERE b.name = '{b}') "
                           f"unwind nodes(p) as node with p, collect(node.name) as paths "
                           f"RETURN Distinct paths, REDUCE(s = 0, d IN RELATIONSHIPS(p) | s + d.cost) AS cost "
                           f"ORDER BY cost ASC LIMIT 1")
    dtf_data = DataFrame([dict(_) for _ in conn.query(findCheapestTrip, db = "lab4")])
    print(dtf_data.get(["paths", "cost"]))

def NumberOfRoutes(a, b):
    numberOfRoutes = (f"MATCH p = (a:City WHERE a.name = '{a}')-[:ROAD*]-(b:City WHERE b.name = '{b}') "
                           f"unwind nodes(p) as node with p, collect(node.name) as paths "
                           f"RETURN Distinct paths, REDUCE(s = 0, d IN RELATIONSHIPS(p) | s + d.cost) AS cost "
                           f"ORDER BY cost ASC")
    dtf_data = DataFrame([dict(_) for _ in conn.query(numberOfRoutes, db = "lab4")])
    print(dtf_data.get(["paths", "cost"]))

def main():
    CreateDB()
    while True:
        print("""\nActions:
    1 - Find route
    2 - Find stops
    3 - Find all routes
    4 - Find cheapest trip
    5 - Find number of routes
    6 - Exit
    """)

        action = input("Choose action: ")
        if (action == '1'):
            bus = input("Enter bus number: ")
            FindRoute(bus)

        elif (action == '2'):
            bus = input("Enter bus number: ")
            FindStops(bus)

        elif (action == '3'):
            a = input("Enter start destination: ")
            b = input("Enter end destination: ")
            FindAllRoutes(a, b)

        elif (action == '4'):
            a = input("Enter start destination: ")
            b = input("Enter end destination: ")
            FindCheapestTrip(a, b)

        elif (action == '5'):
            a = input("Enter start destination: ")
            b = input("Enter end destination: ")
            NumberOfRoutes(a, b)

        elif (action == '6'):
            break;

        else:
            print("Action does not exist")

    conn.close()

if __name__ == '__main__':
    main()

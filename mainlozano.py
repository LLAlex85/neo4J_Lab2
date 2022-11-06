#!/usr/bin/env python3
import csv
import os

from neo4j import GraphDatabase
from neo4j.exceptions import ConstraintError

class MapApp(object):

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self._create_constraints()

    def close(self):
        self.driver.close()

    def _create_constraints(self):
        with self.driver.session() as session:
            session.run("CREATE CONSTRAINT unique_user IF NOT EXISTS FOR (u:User) REQUIRE u.name IS UNIQUE")
            session.run("CREATE CONSTRAINT unique_city IF NOT EXISTS FOR (c:City) REQUIRE c.name IS UNIQUE")
            session.run("CREATE CONSTRAINT unique_restaurant IF NOT EXISTS FOR (r:Restaurant) REQUIRE r.name IS UNIQUE")
            session.run("CREATE CONSTRAINT unique_hotel IF NOT EXISTS FOR (h:Hotel) REQUIRE h.name IS UNIQUE")
            #session.run("CREATE CONSTRAINT unique_used_hashtag_timestamp IF NOT EXISTS FOR ()-[r:USED_HASHTAG]-() REQUIRE r.timestamp IS NOT NULL")


    def _create_user_node(self, name):
        with self.driver.session() as session:
            try:
                session.run("CREATE (u:User {name: $name})", name=name)
            except ConstraintError:
                pass

    def _create_city_node(self, name):
        with self.driver.session() as session:
            try:
                session.run("CREATE (c:City {name: $name})", name=name)
            except ConstraintError:
                pass

    def _create_restaurant_node(self, name, type, address):
        with self.driver.session() as session:
            try:
                session.run("CREATE (r:Restaurant {name: $name, type:$type, address:$address})", name=name, type=type, address=address)
            except ConstraintError:
                pass

    def _create_hotel_node(self, name, stars, phone):
        with self.driver.session() as session:
            try:
                session.run("CREATE (h:Hotel {name: $name, stars:$stars, phone:$phone})", name=name, stars=stars, phone=phone)
            except ConstraintError:
                pass

    def _create_user_to_city_relationship(self, username, city):
        with self.driver.session() as session:
            session.run("""
                MATCH (u:User), (c:City)
                WHERE u.name=$username AND c.name=$city
                CREATE (u)-[r:LIVES]->(c)
                RETURN type(r)""", username=username, city=city)

    def _create_user_to_visited_city_relationship(self, username, city):
        with self.driver.session() as session:
            session.run("""
                MATCH (u:User), (c:City)
                WHERE u.name=$username AND c.name=$city
                CREATE (u)-[r:VISITED]->(c)
                RETURN type(r)""", username=username, city=city)

    def _create_user_to_restaurant_like_relationship(self, username, restaurant):
        with self.driver.session() as session:
            session.run("""
                MATCH (u:User), (v:Restaurant)
                WHERE u.name=$username AND v.name=$restaurant
                CREATE (u)-[r:LIKES]->(v)
                RETURN type(r)""", username=username, restaurant=restaurant)

    def _create_user_to_hotel_likes_relationship(self, username, hotel):
        with self.driver.session() as session:
            session.run("""
                MATCH (u:User), (h:Hotel)
                WHERE u.name=$username AND h.name=$hotel
                CREATE (u)-[r:LIKES]->(h)
                RETURN type(r)""", username=username, hotel=hotel)

    def _create_user_to_hotel_hosted_relationship(self, username, hotel):
        with self.driver.session() as session:
            session.run("""
                MATCH (u:User), (h:Hotel)
                WHERE u.name=$username AND h.name=$hotel
                CREATE (u)-[r:HOSTED]->(h)
                RETURN type(r)""", username=username, hotel=hotel)

    def _create_user_to_hotel_is_at_relationship(self, username, hotel):
        with self.driver.session() as session:
            session.run("""
                MATCH (u:User), (h:Hotel)
                WHERE u.name=$username AND h.name=$hotel
                CREATE (u)-[r:IS_AT]->(h)
                RETURN type(r)""", username=username, hotel=hotel)

    def init(self, source):
        with open(source, newline='') as csv_file:
            reader = csv.DictReader(csv_file,  delimiter='|')
            for r in reader:
               
                self._create_user_node(r["User"])
                self._create_city_node(r["City"])
                self._create_restaurant_node(r["Restaurant"], r["RestaurantType"], r["RestaurantAddress"])
                self._create_hotel_node(r["Hotel"], r["HotelStars"], r["HotelPhone"] )
                self._create_user_to_city_relationship(r["User"], r["City"])
                self._create_user_to_visited_city_relationship(r["User"], r["City"])
                self._create_user_to_restaurant_like_relationship(r["User"], r["Restaurant"])
                self._create_user_to_hotel_likes_relationship(r["User"], r["Hotel"])
                self._create_user_to_hotel_hosted_relationship(r["User"], r["Hotel"])
                self._create_user_to_hotel_is_at_relationship(r["User"], r["Hotel"])
               

if __name__ == "__main__":
    # Read connection env variables
    neo4j_uri = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
    neo4j_user = os.getenv('NEO4J_USER', 'neo4j')
    neo4j_password = os.getenv('NEO4J_PASSWORD', 'lozano')

    twitter = MapApp(neo4j_uri, neo4j_user, neo4j_password)
    twitter.init("data/UserMap.csv")

    twitter.close()


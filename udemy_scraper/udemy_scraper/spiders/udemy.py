import scrapy
from scrapy.exceptions import CloseSpider
import psycopg2

class UdemySpider(scrapy.Spider):
    name = "udemy"
    allowed_domains = ["udemy.com"]
    start_urls = ["https://www.udemy.com/courses/development/"]

    def __init__(self):
        # Connect to PostgreSQL database
        try:
            self.conn = psycopg2.connect(
                dbname="udemy_courses",
                user=" postgres",
                password="etunah@20",
                host="localhost",
                port="5432"
            )
            self.cursor = self.conn.cursor()
        except Exception as e:
            raise CloseSpider(f"Database connection failed: {e}")

    def parse(self, response):
        # Select course containers
        courses = response.css("div.course-card--container--3w8Zm")

        for course in courses:
            title = course.css("div.udlite-focus-visible-target span::text").get()
            description = course.css("p.course-card--course-headline--2DAqq::text").get()
            rating = course.css("span.star-rating--rating-number--3lVe8::text").get()
            price = course.css("div.price-text--price-part--Tu6MH span::text").get()
            instructor = course.css("div.udlite-text-xs.udlite-heading-sm span::text").get()

            # Insert data into PostgreSQL
            self.cursor.execute("""
                INSERT INTO courses (title, description, rating, instructor, price, category)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (title, description, rating, instructor, price, "Development"))
            self.conn.commit()

        # Follow pagination link (if available)
        next_page = response.css("a.udlite-btn.udlite-btn-large.udlite-btn-secondary.udlite-heading-sm.pagination--next--1eRmA::attr(href)").get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)

    def closed(self, reason):
        # Close the database connection
        self.cursor.close()
        self.conn.close()

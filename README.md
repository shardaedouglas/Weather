# Datzilla Flask

## Envirornment Setup

    export FLASK_APP=app
    export FLASK_ENV=development


## Run the Application

Once the above has been set, use this command to start the application
    flask run

## Creating and/or Updating the schema on the database:

NOTE:

SQLAlchemy uses the models defined to create tables.

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///app.db'  <==== points to the database file.
If there's no database file, it will create one.
If there is a current database file it will update/add to the db.


Run Python Intepreter in the app directory
***Copy Paste these lines:***
    from app import db, create_app
    from app.auth.models.auth_models import *
    with create_app().app_context():
	    db.create_all()

This should create a database file in the /instance directory.

## Cybersecurity

Taking User Input

- You use the escape() function you imported earlier to render the word string as text. This is important to avoid Cross Site Scripting (XSS) attacks. If the user submits malicious JavaScript instead of a word, escape() will it render as text and the browser will not run it, keeping your web application safe...


# Dev Notes

## Using Bootstrap Tooltips
Tooltips are enabled via JS at the bottom of the HTML page.

        <script>
            const tooltips = document.querySelectorAll('.tt')
            tooltips.forEach(t => {
              new bootstrap.Tooltip(t)
            })
        </script>


Tooltips are assigned by attaching the project defined "tt" class (to an element?)
<span class="tt" data-bs-placement="auto" title="Tooltip Text"></span>
In this example:

class="tt" = the custom class created for our tooltips
data-bs-placement = Where the tooltip will appear on the page (doesn't seem to be working?)
title = Text for the tooltip to display

Currently, tooltips are enabled for all pages extending from base.html.
References:
https://www.youtube.com/watch?v=WTrW-1JsDYE
https://getbootstrap.com/docs/4.0/components/tooltips/
https://getbootstrap.com/docs/5.0/components/tooltips/


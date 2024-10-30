# Running the application
    $ export FLASK_APP=app
    $ export FLASK_ENV=development
# Once the above has been set, use this command to start the application
    $ flask run

# Cybersecurity
# Taking User Input
    - You use the escape() function you imported earlier to render the word string as text. This is important to avoid Cross Site Scripting (XSS) attacks. If the user submits malicious JavaScript instead of a word, escape() will it render as text and the browser will not run it, keeping your web application safe...
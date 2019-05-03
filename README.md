# Restuarant_Review

running on the server:
cd into folder
Start virtual environment by running `pipenv shell`
Start server with `FLASK_APP=frontend.py python -m flask run --host=0.0.0.0`
On another terminal window, ssh into CDS server, cd into project folder, run `python post_to_frontend.py` to send data to Flask server
Server hosted at `http://128.84.48.178:5000`

Here is the kafka/zookeper setup instructions: https://medium.com/@Ankitthakur/apache-kafka-installation-on-mac-using-homebrew-a367cdefd273

docker run -e HOST=host.docker.internal --expose 5000 -v `pwd`:/tests  -it --rm nyurik/alpine-python3-requests  python /tests/test_messages.py
docker run -e HOST=host.docker.internal --expose 5000 -v `pwd`:/tests  -it --rm nyurik/alpine-python3-requests  python /tests/test_search.py

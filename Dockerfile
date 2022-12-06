FROM mcr.microsoft.com/java/jdk:8-zulu-debian10 as query-analyzer-webservice

ARG SDL_QUERY_ANALYZER_VERSION

RUN mkdir /sdl
WORKDIR /sdl

RUN addgroup --system sdl && adduser --system --ingroup sdl sdl
USER sdl:sdl

COPY "webservice/build/libs/proactive-query-analyzer-webservice-${SDL_QUERY_ANALYZER_VERSION}.jar" "/sdl/proactive-query-analyzer-webservice.jar"
ENTRYPOINT ["java", "-jar", "/sdl/proactive-query-analyzer-webservice.jar"]

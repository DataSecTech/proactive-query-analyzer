FROM mcr.microsoft.com/java/jdk:8-zulu-debian10 as query-analyzer-cli

ARG SDL_QUERY_ANALYZER_VERSION

RUN mkdir /sdl
WORKDIR /sdl

RUN addgroup --system sdl && adduser --system --ingroup sdl sdl
USER sdl:sdl

COPY "query-analyzer/build/libs/proactive-query-analyzer-${SDL_QUERY_ANALYZER_VERSION}.jar" "/sdl/proactive-query-analyzer.jar"
ENTRYPOINT ["java", "-jar", "/sdl/proactive-query-analyzer.jar"]

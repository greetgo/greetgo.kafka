FROM openjdk:8u222-jdk

LABEL maintainer = "greetgo!"

ENV USE_DOCKER yes

ENV PROJECT_DIR=/app

WORKDIR $PROJECT_DIR

COPY build/libs/deps $PROJECT_DIR/deps

COPY execute.sh $PROJECT_DIR/
RUN chmod +x $PROJECT_DIR/execute.sh

COPY build/libs/massive.tests-*.jar $PROJECT_DIR/

RUN ln -s $PROJECT_DIR/massive.tests-*.jar $PROJECT_DIR/massive.tests.jar

ENTRYPOINT ["./execute.sh"]

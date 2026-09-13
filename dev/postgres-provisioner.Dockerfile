FROM postgres:18-alpine
COPY --chmod=0755 wr-cli /usr/local/bin/wr-cli
USER 70:70
ENTRYPOINT ["/usr/local/bin/wr-cli", "postgres"]

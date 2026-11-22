FROM docker.io/denoland/deno:2.6.9

# NAS: run as the DSM user given at build time (APP_UID), so that shared-folder ACLs apply.
ARG APP_UID=1993
RUN if [ "$(id -u deno)" != "$APP_UID" ]; then \
      if command -v usermod >/dev/null 2>&1; then usermod -u "$APP_UID" deno; \
      else sed -i "s/^deno:x:[0-9]*:/deno:x:$APP_UID:/" /etc/passwd; fi \
      && chown -R deno:deno /deno-dir; fi

WORKDIR /app
RUN chown deno:deno /app
RUN mkdir -p /deno-dir/location_data && chown deno:deno /deno-dir/location_data

USER deno

VOLUME /app/dat
VOLUME /app/data

COPY --chown=deno:deno . .

# Deno 2.x: install the exact dependencies recorded in deno.lock. Runtime
# permissions remain attached to `deno task run` rather than the install step.
RUN deno install --frozen

CMD [ "deno", "task", "run" ]

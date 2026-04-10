module.exports = {
  apps: [
    {
      name: "aria-api",
      script: "uvicorn",
      args: "api.main:app --host 0.0.0.0 --port 8000",
      interpreter: "python3",
      cwd: "/var/www/aria",
      env: {
        PYTHONPATH: "/var/www/aria"
      },
      watch: false,
      autorestart: true,
      max_restarts: 10,
      restart_delay: 5000,
      error_file: "/var/log/aria/error.log",
      out_file: "/var/log/aria/out.log",
      log_date_format: "YYYY-MM-DD HH:mm:ss"
    }
  ]
}

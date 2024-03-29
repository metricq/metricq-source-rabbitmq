from setuptools import setup

setup(
    name="metricq_source_rabbitmq",
    version="0.2",
    author="TU Dresden",
    python_requires=">=3.10",
    packages=["metricq_source_rabbitmq"],
    scripts=[],
    entry_points="""
      [console_scripts]
      metricq-source-rabbitmq=metricq_source_rabbitmq:source_cmd
      """,
    install_requires=[
        "aiomonitor",
        "click",
        "click_log",
        "metricq~=5.3",
        "aiohttp",
        "yarl",
    ],
    extras_require={"journallogger": ["systemd"]},
)

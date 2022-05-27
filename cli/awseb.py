import subprocess
import sys


def restart_app_server(awseb_environment_name, awseb_environment_region):
    """Restart the Amazon Web Services Elastic Beanstalk application server with the
    defined name and region.

    Args:
        awseb_environment_name (str): Environment name
        awseb_environment_region (str): Environment AWS region
    """
    try:
        awseb_restart_result = subprocess.run(
            [
                "aws",
                "elasticbeanstalk",
                "restart-app-server",
                "--environment-name",
                awseb_environment_name,
                "--region",
                awseb_environment_region,
            ],
            capture_output=True,
        )
        if awseb_restart_result.returncode != 0:
            print(
                "Could not restart Elastic Beanstalk app server environment"
                f" named `{awseb_environment_name}` in"
                f" region {awseb_environment_region}, see error below for more info:"
            )
            print(awseb_restart_result.stderr.decode(sys.stderr.encoding))
    except Exception as e:
        print(
            "Could not restart Elastic Beanstalk app server environment"
            f" named `{awseb_environment_name}` in"
            f" region {awseb_environment_region}"
        )
        print(e)
        return
    print(
        "Restarted Elastic Beanstalk app server environment"
        f" named `{awseb_environment_name}` in"
        f" region {awseb_environment_region}"
    )

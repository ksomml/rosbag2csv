# ros2bag to csv converter

Converts a ROS2 bag file (.db3) to a CSV file. One CSV file per topic is created.


## Prerequisites

- [ROS 2](https://docs.ros.org/) supported platform, such as [Ubuntu Linux](https://www.releases.ubuntu.com/)

## Usage

(1) Source your workspace with the necessary ROS2 packages (ROS2, Messages):

```bash
$ source /opt/ros/humble/setup.bash
$ source <your_workspace>/install/setup.bash
```

(2) Execute the following command to convert a ROS2 bag file (.db3) to a CSV file:

```bash
$ python3 rosbag2csv.py <bagfile>
```

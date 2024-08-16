#!/usr/bin/env python3
import rclpy
from rclpy.node import Node

from std_msgs.msg import Int32MultiArray


class ArrayPublisher(Node):

    def __init__(self):
        super().__init__('array_publisher')
        self.publisher_ = self.create_publisher(Int32MultiArray, 'array', 10)
        timer_period = 0.5  # seconds
        self.timer = self.create_timer(timer_period, self.timer_callback)
        self.i = 0

    def timer_callback(self):
        msg = Int32MultiArray()
        i = self.i
        msg.data = [i, i, i]
        self.publisher_.publish(msg)
        self.get_logger().info('Publishing: "%s"' % msg.data)
        self.i += 1


def main(args=None):
    rclpy.init(args=args)

    array_publisher = ArrayPublisher()

    rclpy.spin(array_publisher)


if __name__ == '__main__':
    main()

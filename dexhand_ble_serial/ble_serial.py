import rclpy
from rclpy.node import Node
from std_msgs.msg import String
from bleak import BleakClient, BleakScanner
import asyncio
import threading

UART_SERVICE_UUID = "6E400001-B5A3-F393-E0A9-E50E24DCCA9E"
UART_RX_CHAR_UUID = "6E400002-B5A3-F393-E0A9-E50E24DCCA9E"
UART_TX_CHAR_UUID = "6E400003-B5A3-F393-E0A9-E50E24DCCA9E"
DOF_SERVICE_UUID = "1e16c1b4-1936-4f0e-ab62-5e0a702a4935"
DOF_CHAR_UUID = "1e16c1b5-1936-4f0e-ab62-5e0a702a4935"

class BLESerialNode(Node):
    def __init__(self):
        super().__init__('ble_serial')
        self.publisher_ = self.create_publisher(String, 'dexhand_hw_response', 10)
        self.subscription = self.create_subscription(String, 'dexhand_hw_command', self.command_listener_callback, 10)
        self.subscription_dof = self.create_subscription(String, 'dexhand_dof_stream', self.joint_listener_callback, 10)
        self.ble_client = None
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.start_async_loop, daemon=True)
        self.thread.start()

    async def connect_to_ble_device(self):
        devices = await BleakScanner.discover()
        for device in devices:
            if "DexHand" in device.name:
                self.ble_client = BleakClient(device.address, loop=self.loop)
                self.ble_client.set_disconnected_callback(self.on_disconnect)
                await self.ble_client.connect()
                await self.ble_client.start_notify(UART_TX_CHAR_UUID, self.handle_ble_notification)
                self.get_logger().info(f'Connected to {device.name}')
                return True
        return False

    def on_disconnect(self, client):
        self.get_logger().info('BLE device disconnected, attempting to reconnect...')
        asyncio.run_coroutine_threadsafe(self.reconnect(), self.loop)

    async def reconnect(self):
        connected = False
        while not connected:
            try:
                connected = await self.connect_to_ble_device()
                if connected:
                    self.get_logger().info('Reconnected to BLE device.')
                else:
                    self.get_logger().info('Failed to reconnect, retrying in 5 seconds...')
                    await asyncio.sleep(5)
            except Exception as e:
                self.get_logger().error(f'Error during reconnection: {str(e)}')
                await asyncio.sleep(5)

    def command_listener_callback(self, msg):
        self.get_logger().info(f'Sending to BLE device: CMD | {msg.data}')
        asyncio.run_coroutine_threadsafe(self.send_command(msg.data), self.loop)

    async def send_command(self, data):
        try:
            if self.ble_client.is_connected:
                await self.ble_client.write_gatt_char(UART_RX_CHAR_UUID, data.encode('utf-8') + b'\n', response=True)
        except Exception as e:
            self.get_logger().error(f'Failed to write to UART characteristic: {str(e)}')

    def joint_listener_callback(self, msg):
        self.get_logger().info(f'Sending to BLE device: DOF | {msg.data}')
        asyncio.run_coroutine_threadsafe(self.send_dof_data(msg.data), self.loop)

    async def send_dof_data(self, data):
        try:
            if self.ble_client.is_connected:
                message = bytearray.fromhex(data)
                await self.ble_client.write_gatt_char(DOF_CHAR_UUID, message, response=False)
        except Exception as e:
            self.get_logger().error(f'Failed to write to DOF characteristic: {str(e)}')

    def handle_ble_notification(self, sender, data):
        message = data.decode("utf-8").strip()
        self.get_logger().info(f'Received from BLE device: {message}')
        self.publisher_.publish(String(data=message))

    def start_async_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.create_task(self.connect_to_ble_device())
        self.loop.run_forever()

def main(args=None):
    rclpy.init(args=args)
    node = BLESerialNode()
    rclpy.spin(node)
    node.destroy_node()
    rclpy.shutdown()

if __name__ == '__main__':
    main()

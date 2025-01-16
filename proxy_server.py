import asyncio
import socket
import psutil
import logging
from collections import deque
from typing import List, Dict, Deque
import time
from urllib.parse import urlparse
import datetime
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)

class NetworkInterface:
    def __init__(self, name: str, ip: str):
        self.name = name
        self.ip = ip
        self.active_connections = 0
        self.last_used = 0
        self.bytes_sent = 0
        self.bytes_received = 0
        self.total_requests = 0
        self.successful_requests = 0  # Added to track successful requests
        self.failed_requests = 0
        self.last_failure = None
        self.avg_response_time = 0
        self.status = "ACTIVE"  # ACTIVE, DEGRADED, FAILED
        
    def __str__(self):
        return f"Interface({self.name}, {self.ip}, {self.status})"
    
    def update_stats(self, bytes_transferred: int, response_time: float):
        self.bytes_sent += bytes_transferred
        self.total_requests += 1
        # Update moving average of response time
        self.avg_response_time = (self.avg_response_time * (self.total_requests - 1) + response_time) / self.total_requests

    def get_success_rate(self) -> float:
        """Calculate success rate safely"""
        total = self.successful_requests + self.failed_requests
        if total == 0:
            return 0.0
        return (self.successful_requests / total) * 100

    def mark_request_success(self):
        """Mark a request as successful"""
        self.total_requests += 1
        self.successful_requests += 1

    def mark_request_failed(self):
        """Mark a request as failed"""
        self.total_requests += 1
        self.failed_requests += 1

class LoadBalancer:
    def __init__(self):
        self.interfaces: List[NetworkInterface] = []
        self.current_interface_index = 0
        self.failed_interfaces: Dict[str, float] = {}
        self.failure_timeout = 5
        self.max_consecutive_failures = 3
        self.consecutive_failures: Dict[str, int] = {}
        self.stats_interval = 30  # seconds
        self.last_stats_report = 0

    def discover_interfaces(self):
        """Discover and let user select network interfaces"""
        try:
            available_interfaces = []
            network_interfaces = psutil.net_if_addrs()
            
            print("\nAvailable Network Interfaces:")
            print("-----------------------------")
            
            # Show all potentially usable interfaces
            for interface_name, addresses in network_interfaces.items():
                for addr in addresses:
                    if addr.family == socket.AF_INET:
                        # Include all interfaces except localhost
                        if not addr.address.startswith('127.'):
                            # Mark potentially problematic addresses
                            warning = " (Limited connectivity)" if addr.address.startswith('169.254.') else ""
                            available_interfaces.append((interface_name, addr.address))
                            print(f"{len(available_interfaces)}. {interface_name} ({addr.address}){warning}")
            
            if not available_interfaces:
                raise RuntimeError("No network interfaces found")
            
            if len(available_interfaces) == 1:
                print("\nWARNING: Only one interface available. The proxy will work but without load balancing.")
                # Automatically select the only available interface twice
                name, ip = available_interfaces[0]
                self.interfaces.append(NetworkInterface(name, ip))
                self.interfaces.append(NetworkInterface(name, ip))
                logger.info(f"Selected single interface: {name} ({ip})")
                return
            
            # Get user selection
            print("\nSelect interface(s) to use (enter numbers separated by space):")
            print("Note: You can select the same interface twice if needed")
            
            while True:
                try:
                    selections = input("> ").strip().split()
                    if not selections:
                        print("Please select at least one interface")
                        continue
                    
                    if len(selections) > 2:
                        print("Please select maximum 2 interfaces")
                        continue
                        
                    # If user selected one interface, use it twice
                    if len(selections) == 1:
                        selections = [selections[0], selections[0]]
                    
                    idx1, idx2 = map(lambda x: int(x) - 1, selections)
                    if not (0 <= idx1 < len(available_interfaces) and 0 <= idx2 < len(available_interfaces)):
                        print("Invalid selection. Please try again")
                        continue
                    
                    # Add selected interfaces
                    for idx in [idx1, idx2]:
                        name, ip = available_interfaces[idx]
                        self.interfaces.append(NetworkInterface(name, ip))
                        logger.info(f"Selected interface: {name} ({ip})")
                    break
                    
                except ValueError:
                    print("Invalid input. Please enter numbers only")
                    continue
                    
        except Exception as e:
            logger.error(f"Error discovering interfaces: {e}")
            raise

    def mark_interface_failed(self, interface: NetworkInterface, error: str):
        """Track interface failures with detailed reporting"""
        interface.mark_request_failed()
        interface.last_failure = time.time()
        self.consecutive_failures[interface.ip] = self.consecutive_failures.get(interface.ip, 0) + 1
        
        if self.consecutive_failures[interface.ip] >= self.max_consecutive_failures:
            self.failed_interfaces[interface.ip] = time.time()
            interface.status = "FAILED"
            logger.warning(
                f"Interface {interface.name} ({interface.ip}) marked as FAILED:\n"
                f"  - Consecutive failures: {self.consecutive_failures[interface.ip]}\n"
                f"  - Last error: {error}\n"
                f"  - Success rate: {interface.get_success_rate():.1f}%\n"
                f"  - Average response time: {interface.avg_response_time:.2f}s\n"
                f"Switching to backup interface..."
            )
            self.consecutive_failures[interface.ip] = 0
        else:
            interface.status = "DEGRADED"
            logger.info(
                f"Interface {interface.name} degraded performance:\n"
                f"  - Failure count: {self.consecutive_failures[interface.ip]}/{self.max_consecutive_failures}\n"
                f"  - Error: {error}"
            )

    def report_stats(self):
        """Generate periodic interface statistics report"""
        current_time = time.time()
        if current_time - self.last_stats_report >= self.stats_interval:
            logger.info("\n=== Interface Statistics Report ===")
            for interface in self.interfaces:
                status_color = {
                    "ACTIVE": "✓",
                    "DEGRADED": "⚠",
                    "FAILED": "✗"
                }.get(interface.status, "?")
                
                logger.info(
                    f"\nInterface: {interface.name} ({interface.ip}) {status_color}\n"
                    f"  Status: {interface.status}\n"
                    f"  Active connections: {max(0, interface.active_connections)}\n"
                    f"  Total requests: {interface.total_requests}\n"
                    f"  Successful requests: {interface.successful_requests}\n"
                    f"  Failed requests: {interface.failed_requests}\n"
                    f"  Success rate: {interface.get_success_rate():.1f}%\n"
                    f"  Average response time: {interface.avg_response_time:.2f}s\n"
                    f"  Data transferred: {self.format_bytes(interface.bytes_sent)}"
                )
            logger.info("=" * 30)
            self.last_stats_report = current_time

    @staticmethod
    def format_bytes(bytes_count: int) -> str:
        """Format bytes to human readable format"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if bytes_count < 1024:
                return f"{bytes_count:.1f} {unit}"
            bytes_count /= 1024
        return f"{bytes_count:.1f} TB"

    def is_interface_failed(self, interface: NetworkInterface) -> bool:
        """Check if interface is currently marked as failed"""
        if interface.ip in self.failed_interfaces:
            # Check if enough time has passed to retry
            if time.time() - self.failed_interfaces[interface.ip] > self.failure_timeout:
                del self.failed_interfaces[interface.ip]
                return False
            return True
        return False

    def get_best_interface(self) -> NetworkInterface:
        """Quick interface selection with fast failover"""
        if not self.interfaces:
            raise RuntimeError("No interfaces available")

        # Filter out invalid IP addresses (169.254.x.x)
        valid_interfaces = [
            interface for interface in self.interfaces 
            if not interface.ip.startswith('169.254.')
        ]

        if not valid_interfaces:
            raise RuntimeError("No valid interfaces available. Please select interfaces with valid IP addresses.")

        # Try interfaces in round-robin fashion
        for _ in range(len(valid_interfaces)):
            interface = valid_interfaces[self.current_interface_index % len(valid_interfaces)]
            self.current_interface_index = (self.current_interface_index + 1) % len(valid_interfaces)
            
            if not self.is_interface_failed(interface):
                return interface
                
        # If all interfaces are failed, reset and try again
        self.failed_interfaces.clear()
        self.consecutive_failures.clear()
        return valid_interfaces[0]

class ProxyServer:
    def __init__(self, host: str = '127.0.0.1', port: int = 8080):
        self.host = host
        self.port = port
        self.load_balancer = LoadBalancer()
        # Add log directory setup
        self.log_dir = "proxy_logs"
        os.makedirs(self.log_dir, exist_ok=True)
        self.log_file = os.path.join(
            self.log_dir,
            f"proxy_log_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        )
        
    async def handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter
    ):
        """Handle individual client connections with detailed monitoring"""
        interface = None
        remote_writer = None
        tasks = []
        start_time = time.time()
        bytes_transferred = 0
        
        try:
            try:
                interface = self.load_balancer.get_best_interface()
            except RuntimeError as e:
                logger.error(f"Interface selection failed: {e}")
                writer.write(b'HTTP/1.1 503 Service Unavailable\r\n\r\n')
                await writer.drain()
                return

            client_addr = writer.get_extra_info('peername')
            
            # Log new connection
            self.log_event(
                "CONNECTION", 
                f"New client {client_addr}",
                interface
            )

            # Read the initial request with timeout
            try:
                request_data = await asyncio.wait_for(reader.read(8192), timeout=5.0)
                request_text = request_data.decode('utf-8', errors='ignore')
            except asyncio.TimeoutError:
                logger.error("Timeout reading request")
                return
            
            # Quick parse of the request
            first_line = request_text.split('\n')[0].strip()
            try:
                method, url, protocol = first_line.split(' ')
            except ValueError:
                logger.error(f"Invalid request format: {first_line}")
                return
            
            # Fast path for CONNECT
            try:
                if method == 'CONNECT':
                    host, port = url.split(':')
                    port = int(port)
                else:
                    # Quick host extraction
                    host_start = request_text.find('Host: ') + 6
                    if host_start > 5:
                        host_end = request_text.find('\r\n', host_start)
                        host = request_text[host_start:host_end].strip()
                        port = 80
                    else:
                        parsed_url = urlparse(url)
                        host = parsed_url.netloc
                        port = parsed_url.port or (443 if url.startswith('https') else 80)
            except Exception as e:
                logger.error(f"Error parsing request: {e}")
                return

            # Fast connection attempt with immediate failover
            async def try_connect(interface):
                try:
                    return await asyncio.wait_for(
                        asyncio.open_connection(
                            host=host,
                            port=port,
                            local_addr=(interface.ip, 0)
                        ),
                        timeout=2.0
                    )
                except Exception as e:
                    logger.error(f"Quick connection failed via {interface}: {e}")
                    self.load_balancer.mark_interface_failed(interface, str(e))
                    return None

            # Try all interfaces quickly
            remote_reader = None
            for _ in range(len(self.load_balancer.interfaces)):
                result = await try_connect(interface)
                if result:
                    remote_reader, remote_writer = result
                    break
                interface = self.load_balancer.get_best_interface()
            
            if not remote_writer:
                writer.write(b'HTTP/1.1 502 Bad Gateway\r\n\r\n')
                await writer.drain()
                return

            # Connection successful, handle the request
            try:
                if method == 'CONNECT':
                    writer.write(b'HTTP/1.1 200 Connection established\r\n\r\n')
                else:
                    remote_writer.write(request_data)
                await writer.drain()

                # Create forwarding tasks
                client_to_server = asyncio.create_task(
                    self.forward(reader, remote_writer, 'client → server', interface),
                    name=f"c2s_{host}:{port}"
                )
                server_to_client = asyncio.create_task(
                    self.forward(remote_reader, writer, 'server → client', interface),
                    name=f"s2c_{host}:{port}"
                )
                tasks.extend([client_to_server, server_to_client])

                # Wait for either task to complete (or fail)
                done, pending = await asyncio.wait(
                    tasks,
                    return_when=asyncio.FIRST_COMPLETED
                )

                # Cancel pending tasks
                for task in pending:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

            except Exception as e:
                logger.error(f"Error in connection handling: {e}")

            # Update interface statistics
            end_time = time.time()
            response_time = end_time - start_time
            interface.update_stats(bytes_transferred, response_time)
            
            # Generate periodic statistics report
            self.load_balancer.report_stats()

            # Update success statistics on successful connection
            if remote_writer:
                interface.mark_request_success()

        except Exception as e:
            if interface:
                self.load_balancer.mark_interface_failed(interface, str(e))
            logger.error(f"Connection error: {e}")
            
        finally:
            # Clean up tasks
            for task in tasks:
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                    except Exception as e:
                        logger.error(f"Error cancelling task {task.get_name()}: {e}")

            # Clean up connections
            if interface:
                interface.active_connections = max(0, interface.active_connections - 1)
            
            for w in [writer, remote_writer]:
                if w and not w.is_closing():
                    try:
                        w.close()
                        await asyncio.wait_for(w.wait_closed(), timeout=1.0)
                    except Exception:
                        pass

    async def forward(self, reader, writer, direction, interface: NetworkInterface = None):
        """Optimized data forwarding with monitoring"""
        bytes_count = 0
        try:
            while True:
                try:
                    data = await asyncio.wait_for(reader.read(32768), timeout=10.0)
                    if not data:
                        break
                    bytes_count += len(data)
                    writer.write(data)
                    await writer.drain()
                except asyncio.TimeoutError:
                    # On timeout
                    self.log_event(
                        "TIMEOUT",
                        f"{direction} after {self.load_balancer.format_bytes(bytes_count)}",
                        interface,
                        "WARNING"
                    )
                    break
                except ConnectionResetError:
                    # On connection reset
                    self.log_event(
                        "RESET",
                        f"{direction} after {self.load_balancer.format_bytes(bytes_count)}",
                        interface,
                        "WARNING"
                    )
                    break
                except Exception as e:
                    logger.error(
                        f"Error forwarding {direction}: {e}\n"
                        f"  Interface: {interface.name if interface else 'unknown'}\n"
                        f"  Bytes transferred: {self.load_balancer.format_bytes(bytes_count)}"
                    )
                    break
            return bytes_count
        except asyncio.CancelledError:
            logger.debug(f"Forward {direction} cancelled after {self.load_balancer.format_bytes(bytes_count)}")
            raise

    async def start(self):
        """Start the proxy server"""
        try:
            print("\nProxy Server Configuration")
            print("-------------------------")
            
            # Let user configure port
            while True:
                try:
                    port_input = input("Enter port number (default 8080): ").strip()
                    if not port_input:
                        break
                    port = int(port_input)
                    if 1024 <= port <= 65535:
                        self.port = port
                        break
                    print("Port must be between 1024 and 65535")
                except ValueError:
                    print("Invalid port number")
            
            # Discover and select interfaces
            self.load_balancer.discover_interfaces()
            
            server = await asyncio.start_server(
                self.handle_client,
                self.host,
                self.port
            )
            
            print("\nProxy Server Status")
            print("------------------")
            logger.info(f"Proxy server started on {self.host}:{self.port}")
            logger.info("Combined interfaces:")
            for interface in self.load_balancer.interfaces:
                logger.info(f"  - {interface}")
            logger.info(f"Logging requests to: {self.log_file}")
            print("\nTo configure Chrome:")
            print(f"1. Go to Settings -> System -> Open proxy settings")
            print(f"2. Set HTTP and HTTPS proxy to: {self.host}:{self.port}")
            print("\nPress Ctrl+C to stop the server")
                
            async with server:
                await server.serve_forever()
                
        except Exception as e:
            logger.error(f"Server error: {e}")
            raise

    def log_event(self, event_type: str, details: str, interface=None, status="INFO"):
        """Log events in a consistent one-line format"""
        interface_info = f"[{interface.name}:{interface.ip}]" if interface else "[no-interface]"
        logger.info(f"{status} | {event_type} | {interface_info} | {details}")

async def main():
    proxy = ProxyServer()
    await proxy.start()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")

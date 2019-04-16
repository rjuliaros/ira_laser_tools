#include <ros/ros.h>
#include <string.h>
#include <tf/transform_listener.h>
#include <pcl_ros/transforms.h>
#include <laser_geometry/laser_geometry.h>
#include <pcl/conversions.h>
#include <pcl/point_cloud.h>
#include <pcl/point_types.h>
#include <pcl/io/pcd_io.h>
#include <sensor_msgs/PointCloud.h>
#include <sensor_msgs/PointCloud2.h>
#include <sensor_msgs/point_cloud_conversion.h>
#include "sensor_msgs/LaserScan.h"
#include "pcl_ros/point_cloud.h"
#include <Eigen/Dense>
#include <dynamic_reconfigure/server.h>
#include <ira_laser_tools/LaserscanMultiMergerConfig.h>
#include <sstream>

class LaserscanMerger
{
public:
  LaserscanMerger();
  void scanCallback(const sensor_msgs::LaserScan& scan, std::string topic);
  void pointcloudToLaserscan(const pcl::PointCloud<pcl::PointXYZI>& merge, sensor_msgs::LaserScan& scan);
  void reconfigureCallback(laserscan_multi_merger::LaserscanMultiMergerConfig& config, uint32_t level);

private:
  ros::NodeHandle node_;
  ros::NodeHandle private_node_;
  laser_geometry::LaserProjection projector_;
  tf::TransformListener tf_listener_;

  ros::Publisher pointcloud_publisher_;
  ros::Publisher laserscan_publisher_;
  std::vector<ros::Subscriber> scan_subscribers;
  std::vector<bool> clouds_modified;

  std::map<std::string, sensor_msgs::LaserScan> input_scans_;
  std::map<std::string, pcl::PointCloud<pcl::PointXYZI>> output_clouds_;

  std::vector<std::string> input_topics;

  void laserscanTopicParser();

  double angle_min_;
  double angle_max_;
  double angle_increment_;
  double time_increment_;
  double scan_time_;
  double range_min_;
  double range_max_;

  std::string destination_frame_;
  std::string fixed_frame_;
  std::string cloud_destination_topic_;
  std::string scan_destination_topic_;
  bool check_topic_type;
  std::string laserscan_topics_;
};

void LaserscanMerger::reconfigureCallback(laserscan_multi_merger::LaserscanMultiMergerConfig& config, uint32_t level)
{
  this->angle_min_ = config.angle_min;
  this->angle_max_ = config.angle_max;
  this->angle_increment_ = config.angle_increment;
  this->time_increment_ = config.time_increment;
  this->scan_time_ = config.scan_time;
  this->range_min_ = config.range_min;
  this->range_max_ = config.range_max;
}

void LaserscanMerger::laserscanTopicParser()
{
  // LaserScan topics to subscribe
  ros::master::V_TopicInfo topics;
  ros::master::getTopics(topics);

  std::istringstream iss(laserscan_topics_);
  std::vector<std::string> tokens;
  std::copy(std::istream_iterator<std::string>(iss), std::istream_iterator<std::string>(),
            std::back_inserter<std::vector<std::string>>(tokens));

  std::vector<std::string> tmp_input_topics;

  if (check_topic_type == false)
  {
    tmp_input_topics = tokens;
  }
  else
  {
    for (int i = 0; i < tokens.size(); ++i)
    {
      for (int j = 0; j < topics.size(); ++j)
      {
        if ((topics[j].name.compare(node_.resolveName(tokens[i])) == 0) &&
            (topics[j].datatype.compare("sensor_msgs/LaserScan") == 0))
        {
          tmp_input_topics.push_back(topics[j].name);
        }
      }
    }
  }

  sort(tmp_input_topics.begin(), tmp_input_topics.end());
  std::vector<std::string>::iterator last = std::unique(tmp_input_topics.begin(), tmp_input_topics.end());
  tmp_input_topics.erase(last, tmp_input_topics.end());

  // Do not re-subscribe if the topics are the same
  if ((tmp_input_topics.size() != input_topics.size()) ||
      !equal(tmp_input_topics.begin(), tmp_input_topics.end(), input_topics.begin()))
  {
    // Unsubscribe from previous topics
    for (int i = 0; i < scan_subscribers.size(); ++i)
      scan_subscribers[i].shutdown();

    input_topics = tmp_input_topics;
    if (input_topics.size() > 0)
    {
      scan_subscribers.resize(input_topics.size());
      clouds_modified.resize(input_topics.size());
      std::ostringstream ss;
      ss << "Subscribing to " << scan_subscribers.size() << " topics:";
      //            ROS_INFO("Subscribing to %f topics:", scan_subscribers.size());
      for (int i = 0; i < input_topics.size(); ++i)
      {
        // this node_.subscribe<MsgType, const MsgType&> is needed to bind to a callback which receives a const Type&
        // otherwise, we are forced to subscribe to callback which receives a shared_ptr
        scan_subscribers[i] = node_.subscribe<sensor_msgs::LaserScan, const sensor_msgs::LaserScan&>(
            input_topics[i], 1, boost::bind(&LaserscanMerger::scanCallback, this, _1, input_topics[i]));
        clouds_modified[i] = false;
        ss << " " << input_topics[i];
      }
      ROS_INFO_STREAM(ss.str());
    }
    else
      ROS_INFO("Not subscribed to any topic.");
  }
}

LaserscanMerger::LaserscanMerger() : node_(""), private_node_("~")
{
  private_node_.getParam("destination_frame", destination_frame_);
  private_node_.getParam("fixed_frame", fixed_frame_);
  private_node_.getParam("cloud_destination_topic", cloud_destination_topic_);
  private_node_.getParam("scan_destination_topic", scan_destination_topic_);

  private_node_.getParam("laserscan_topics", laserscan_topics_);

  this->laserscanTopicParser();

  pointcloud_publisher_ = private_node_.advertise<sensor_msgs::PointCloud2>(cloud_destination_topic_, 1, false);
  laserscan_publisher_ = private_node_.advertise<sensor_msgs::LaserScan>(scan_destination_topic_, 1, false);
}

void LaserscanMerger::scanCallback(const sensor_msgs::LaserScan& scan, std::string topic)
{
  // Verify that TF knows how to transform from the received scan to the fixed scan frame
  // as we are using a high precission projector, we need the tf at the time of the last
  // scan
  try
  {
    ros::Time end_stamp = (scan.header.stamp + ros::Duration().fromSec((scan.ranges.size() - 1) * scan.time_increment));
    if (!tf_listener_.waitForTransform(fixed_frame_, scan.header.frame_id, end_stamp, ros::Duration(10)))
      return;
  }
  catch (const tf::TransformException& ex)
  {
    ROS_ERROR("%s", ex.what());
    return;
  }

  input_scans_[topic] = scan;
  sensor_msgs::PointCloud2 scan_in_fixed_frame;
  try
  {
    projector_.transformLaserScanToPointCloud(fixed_frame_, scan, scan_in_fixed_frame, tf_listener_);
    // need this weird if. otherwise, in case of no intensities on input, it will keep printing at each iteration
    // a warning message about intensities missing field
    if (scan.intensities.size() != 0)
    {
      pcl::fromROSMsg(scan_in_fixed_frame, output_clouds_[topic]);
    }
    else
    {
      pcl::PointCloud<pcl::PointXYZ> cloud_without_intensities;
      pcl::fromROSMsg(scan_in_fixed_frame, cloud_without_intensities);
      pcl::copyPointCloud(cloud_without_intensities, output_clouds_[topic]);
    }
  }
  catch (const tf::TransformException& ex)
  {
    ROS_ERROR("%s", ex.what());
    return;
  }

  for (int i = 0; i < input_topics.size(); ++i)
  {
    if (topic.compare(input_topics[i]) == 0)
    {
      clouds_modified[i] = true;
    }
  }

  // Count how many scans we have
  int totalClouds = 0;
  for (int i = 0; i < clouds_modified.size(); ++i)
    if (clouds_modified[i])
      ++totalClouds;

  // Go ahead only if all subscribed scans have arrived
  if (totalClouds == clouds_modified.size())
  {
    ros::Time current_stamp;
    pcl_conversions::fromPCL(std::min_element(output_clouds_.begin(), output_clouds_.end(),
                                              [](std::pair<std::string, pcl::PointCloud<pcl::PointXYZI>> n,
                                                 std::pair<std::string, pcl::PointCloud<pcl::PointXYZI>> m) {
                                                return n.second.header.stamp < m.second.header.stamp;
                                              })
                                 ->second.header.stamp,
                             current_stamp);

    int first = 0;
    // for (first = 0; first < clouds_modified.size(); first++)
    //  if (output_clouds_[first].points.size() != 0)
    //    break;

    if (first == clouds_modified.size())
    {
      ROS_INFO_THROTTLE(5, "Input is empty (either laserscan was empty or couldn't be transformed into fixed frame)");
      return;
    }

    for (int i = first; i < clouds_modified.size(); ++i)
    {
      clouds_modified[i] = false;
    }

    pcl::PointCloud<pcl::PointXYZI> merge;
    merge.header.frame_id = destination_frame_;
    merge.header.stamp = pcl_conversions::toPCL(current_stamp);
    for (auto& c : output_clouds_)
    {
      pcl::PointCloud<pcl::PointXYZI> transformed;
      pcl_ros::transformPointCloud(destination_frame_, current_stamp, c.second, fixed_frame_, transformed,
                                   tf_listener_);
      merge += transformed;
    }

    pointcloud_publisher_.publish(merge);

    sensor_msgs::LaserScan output_scan;
    output_scan.header.frame_id = destination_frame_;
    output_scan.header.stamp = current_stamp;
    pointcloudToLaserscan(merge, output_scan);

    laserscan_publisher_.publish(output_scan);
  }
}

void LaserscanMerger::pointcloudToLaserscan(const pcl::PointCloud<pcl::PointXYZI>& merge,
                                            sensor_msgs::LaserScan& output)
{
  output.angle_min = this->angle_min_;
  output.angle_max = this->angle_max_;
  output.angle_increment = this->angle_increment_;
  output.time_increment = this->time_increment_;
  output.scan_time = this->scan_time_;
  output.range_min = this->range_min_;
  output.range_max = this->range_max_;

  uint32_t ranges_size = std::ceil((output.angle_max - output.angle_min) / output.angle_increment);
  output.ranges.assign(ranges_size, output.range_max + 1.0);

  output.intensities.assign(ranges_size, 0);
  for (auto& point : merge.points)
  {
    const float& x = point.x;
    const float& y = point.y;
    const float& z = point.z;
    const float& intensity = point.intensity;
    if (std::isnan(x) || std::isnan(y) || std::isnan(z))
    {
      ROS_DEBUG("rejected for nan in point(%f, %f, %f)\n", x, y, z);
      continue;
    }

    double range_sq = y * y + x * x;
    double range_min_sq_ = output.range_min * output.range_min;
    if (range_sq < range_min_sq_)
    {
      ROS_DEBUG("rejected for range %f below minimum value %f. Point: (%f, %f, %f)", range_sq, range_min_sq_, x, y, z);
      continue;
    }

    double angle = atan2(y, x);
    if (angle < output.angle_min || angle > output.angle_max)
    {
      ROS_DEBUG("rejected for angle %f not in range (%f, %f)\n", angle, output.angle_min, output.angle_max);
      continue;
    }
    int index = (angle - output.angle_min) / output.angle_increment;

    if (output.ranges[index] * output.ranges[index] > range_sq)
    {
      output.ranges[index] = sqrt(range_sq);
      output.intensities[index] = intensity;
    }
  }
}

int main(int argc, char** argv)
{
  ros::init(argc, argv, "laser_multi_merger");

  LaserscanMerger laser_merger;

  dynamic_reconfigure::Server<laserscan_multi_merger::LaserscanMultiMergerConfig> reconfigure_server;
  dynamic_reconfigure::Server<laserscan_multi_merger::LaserscanMultiMergerConfig>::CallbackType reconfigure_callback;

  reconfigure_callback = boost::bind(&LaserscanMerger::reconfigureCallback, &laser_merger, _1, _2);
  reconfigure_server.setCallback(reconfigure_callback);

  ros::spin();

  return 0;
}

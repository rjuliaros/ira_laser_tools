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
#include <ira_laser_tools/laserscan_multi_mergerConfig.h>
#include <sstream>

using namespace std;
using namespace pcl;
using namespace laserscan_multi_merger;

class LaserscanMerger
{
public:
    LaserscanMerger();
    void scanCallback(const sensor_msgs::LaserScan::ConstPtr& scan, std::string topic);
//    void pointcloud_to_laserscan(Eigen::MatrixXf points, pcl::PCLPointCloud2 *merged_cloud);
    void pointcloud_to_laserscan(Eigen::MatrixXf points, pcl::PCLPointCloud2 *merged_cloud, ros::Time & stamp);
    void reconfigureCallback(laserscan_multi_mergerConfig &config, uint32_t level);

private:
    ros::NodeHandle node_;
    ros::NodeHandle private_node_;
    laser_geometry::LaserProjection projector_;
    tf::TransformListener tfListener_;

    ros::Publisher point_cloud_publisher_;
    ros::Publisher laser_scan_publisher_;
    vector<ros::Subscriber> scan_subscribers;
    vector<bool> clouds_modified;

    vector<pcl::PCLPointCloud2> pcl_clouds;
    vector<sensor_msgs::PointCloud> sensor_clouds;
    vector<string> input_topics;

    void laserscan_topic_parser();

    double angle_min;
    double angle_max;
    double angle_increment;
    double time_increment;
    double scan_time;
    double range_min;
    double range_max;

    string destination_frame;
    string fixed_frame;
    string cloud_destination_topic;
    string scan_destination_topic;
    bool check_topic_type;
    string laserscan_topics;
};

void LaserscanMerger::reconfigureCallback(laserscan_multi_mergerConfig &config, uint32_t level)
{
	this->angle_min = config.angle_min;
	this->angle_max = config.angle_max;
	this->angle_increment = config.angle_increment;
	this->time_increment = config.time_increment;
	this->scan_time = config.scan_time;
	this->range_min = config.range_min;
	this->range_max = config.range_max;
}

void LaserscanMerger::laserscan_topic_parser()
{
	// LaserScan topics to subscribe
	ros::master::V_TopicInfo topics;
	ros::master::getTopics(topics);

    istringstream iss(laserscan_topics);
	vector<string> tokens;
	copy(istream_iterator<string>(iss), istream_iterator<string>(), back_inserter<vector<string> >(tokens));

	vector<string> tmp_input_topics;

	if (check_topic_type == false) {
	     tmp_input_topics = tokens;
	}
	else {
		for(int i=0;i<tokens.size();++i)
		{
		    for(int j=0;j<topics.size();++j)
			{
				if((topics[j].name.compare(node_.resolveName(tokens[i])) == 0) && (topics[j].datatype.compare("sensor_msgs/LaserScan") == 0) )
				{
					tmp_input_topics.push_back(topics[j].name);
				}
			}
		}
	}

	sort(tmp_input_topics.begin(),tmp_input_topics.end());
	std::vector<string>::iterator last = std::unique(tmp_input_topics.begin(), tmp_input_topics.end());
	tmp_input_topics.erase(last, tmp_input_topics.end());


	// Do not re-subscribe if the topics are the same
	if( (tmp_input_topics.size() != input_topics.size()) || !equal(tmp_input_topics.begin(),tmp_input_topics.end(),input_topics.begin()))
	{

		// Unsubscribe from previous topics
		for(int i=0; i<scan_subscribers.size(); ++i)
			scan_subscribers[i].shutdown();

		input_topics = tmp_input_topics;
		if(input_topics.size() > 0)
		{
            scan_subscribers.resize(input_topics.size());
			clouds_modified.resize(input_topics.size());
			pcl_clouds.resize(input_topics.size());
			sensor_clouds.resize(input_topics.size());
			ostringstream ss;
			ss << "Subscribing to " << scan_subscribers.size() << " topics:";
//            ROS_INFO("Subscribing to %f topics:", scan_subscribers.size());
			for(int i=0; i<input_topics.size(); ++i)
			{
                scan_subscribers[i] = node_.subscribe<sensor_msgs::LaserScan> (input_topics[i].c_str(), 1, boost::bind(&LaserscanMerger::scanCallback,this, _1, input_topics[i]));
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
	private_node_.getParam("destination_frame", destination_frame);
	private_node_.getParam("fixed_frame", fixed_frame);
	private_node_.getParam("cloud_destination_topic", cloud_destination_topic);
	private_node_.getParam("scan_destination_topic", scan_destination_topic);
    private_node_.getParam("laserscan_topics", laserscan_topics);

    this->laserscan_topic_parser();

	point_cloud_publisher_ = private_node_.advertise<sensor_msgs::PointCloud2> (cloud_destination_topic.c_str(), 1, false);
	laser_scan_publisher_ = private_node_.advertise<sensor_msgs::LaserScan> (scan_destination_topic.c_str(), 1, false);
}

void LaserscanMerger::scanCallback(const sensor_msgs::LaserScan::ConstPtr& scan, std::string topic)
{
	sensor_msgs::PointCloud tmpCloud1,tmpCloud2;
	sensor_msgs::PointCloud2 tmpCloud3;

        // Verify that TF knows how to transform from the received scan to the fixed scan frame
        // as we are using a high precission projector, we need the tf at the time of the last
        // scan
	try
	{
            ros::Time end_stamp = (scan->header.stamp + ros::Duration().fromSec( (scan->ranges.size()-1) * scan->time_increment));
	    if(!tfListener_.waitForTransform(fixed_frame.c_str(), scan->header.frame_id.c_str(), end_stamp, ros::Duration(10)))
	        return;
	}catch (tf::TransformException ex){ROS_ERROR("%s",ex.what());return;}
	try
	{
	    projector_.transformLaserScanToPointCloud(fixed_frame.c_str(), *scan, tmpCloud1, tfListener_);
	}catch (tf::TransformException ex){
		ROS_ERROR("%s",ex.what());
		return;
	}
	try
	{
	 	tfListener_.transformPointCloud(scan->header.frame_id.c_str(), tmpCloud1, tmpCloud2); //this function always fails the first time it is called, don't know why
	}catch (tf::TransformException ex){ROS_ERROR("%s",ex.what());return;}
			sensor_msgs::convertPointCloudToPointCloud2(tmpCloud2,tmpCloud3);


	for(int i=0; i<input_topics.size(); ++i)
	{
		if(topic.compare(input_topics[i]) == 0)
		{
			//sensor_msgs::convertPointCloudToPointCloud2(tmpCloud2,tmpCloud3);
			//pcl_conversions::toPCL(tmpCloud3, pcl_clouds[i]);
			sensor_clouds[i] = tmpCloud2;
			clouds_modified[i] = true;
		}
	}	

    // Count how many scans we have
	int totalClouds = 0;
	for(int i=0; i<clouds_modified.size(); ++i)
		if(clouds_modified[i])
			++totalClouds;

    // Go ahead only if all subscribed scans have arrived
	if(totalClouds == clouds_modified.size())
	{
	    
	    ros::Time current_stamp;
	    string error_msg;
	    tfListener_.getLatestCommonTime(destination_frame, fixed_frame, current_stamp, &error_msg);
	    if (current_stamp == ros::Time(0))
	    {
	        ROS_WARN("Error getting latest common time between %s and %s", destination_frame.c_str(), fixed_frame.c_str());
            return;
	    }
	    // current stamp tiene que ser el ultimo tiempo comun
		pcl::PCLPointCloud2 merged_cloud;
		int first = 0;
		for (first = 0; first < clouds_modified.size(); first++)
			if (sensor_clouds[first].points.size() != 0)
break;
if (first == clouds_modified.size())
{
ROS_INFO_THROTTLE(5,"Empty clouds!");
return;
}
		for(int i=first; i<clouds_modified.size(); ++i)
		{
	 	    tfListener_.transformPointCloud(destination_frame.c_str(), current_stamp, sensor_clouds[i], fixed_frame, tmpCloud2); //this function always fails the first time it is called, don't know why
			
			sensor_msgs::convertPointCloudToPointCloud2(tmpCloud2,tmpCloud3);
			pcl_conversions::toPCL(tmpCloud3, pcl_clouds[i]);
		
			pcl::concatenatePointCloud(merged_cloud, pcl_clouds[i], merged_cloud);
			clouds_modified[i] = false;
		}
	
		point_cloud_publisher_.publish(merged_cloud);

		Eigen::MatrixXf points;
		getPointCloudAsEigen(merged_cloud,points);

        // a este le tenemos que pasar el ultimo timepo comun.
		pointcloud_to_laserscan(points, &merged_cloud, current_stamp);
	}
}

void LaserscanMerger::pointcloud_to_laserscan(Eigen::MatrixXf points, pcl::PCLPointCloud2 *merged_cloud, ros::Time & stamp)
{
	sensor_msgs::LaserScanPtr output(new sensor_msgs::LaserScan());
	output->header = pcl_conversions::fromPCL(merged_cloud->header);
	output->header.frame_id = destination_frame.c_str();
	output->header.stamp = stamp;  //fixes #265
	output->angle_min = this->angle_min;
	output->angle_max = this->angle_max;
	output->angle_increment = this->angle_increment;
	output->time_increment = this->time_increment;
	output->scan_time = this->scan_time;
	output->range_min = this->range_min;
	output->range_max = this->range_max;

	uint32_t ranges_size = std::ceil((output->angle_max - output->angle_min) / output->angle_increment);
	output->ranges.assign(ranges_size, output->range_max + 1.0);

	for(int i=0; i<points.cols(); i++)
	{
		const float &x = points(0,i);
		const float &y = points(1,i);
		const float &z = points(2,i);

		if ( std::isnan(x) || std::isnan(y) || std::isnan(z) )
		{
			ROS_DEBUG("rejected for nan in point(%f, %f, %f)\n", x, y, z);
			continue;
		}

		double range_sq = y*y+x*x;
		double range_min_sq_ = output->range_min * output->range_min;
		if (range_sq < range_min_sq_) {
			ROS_DEBUG("rejected for range %f below minimum value %f. Point: (%f, %f, %f)", range_sq, range_min_sq_, x, y, z);
			continue;
		}

		double angle = atan2(y, x);
		if (angle < output->angle_min || angle > output->angle_max)
		{
			ROS_DEBUG("rejected for angle %f not in range (%f, %f)\n", angle, output->angle_min, output->angle_max);
			continue;
		}
		int index = (angle - output->angle_min) / output->angle_increment;


		if (output->ranges[index] * output->ranges[index] > range_sq)
			output->ranges[index] = sqrt(range_sq);
	}

	laser_scan_publisher_.publish(output);
}

int main(int argc, char** argv)
{
	ros::init(argc, argv, "laser_multi_merger");

    LaserscanMerger _laser_merger;

    dynamic_reconfigure::Server<laserscan_multi_mergerConfig> server;
    dynamic_reconfigure::Server<laserscan_multi_mergerConfig>::CallbackType f;

    f = boost::bind(&LaserscanMerger::reconfigureCallback,&_laser_merger, _1, _2);
	server.setCallback(f);

	ros::spin();

	return 0;
}

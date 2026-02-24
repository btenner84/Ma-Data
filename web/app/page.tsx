"use client";

import { useQuery } from "@tanstack/react-query";
import Link from "next/link";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
} from "recharts";
import { enrollmentAPI, starsAPI, lookupAPI } from "@/lib/api";
import { Users, Star, TrendingUp, ArrowRight, Building2, Calendar } from "lucide-react";

const STAR_COLORS = {
  "1": "#dc2626",
  "2": "#ea580c",
  "3": "#eab308",
  "4": "#22c55e",
  "5": "#16a34a",
};

function formatNumber(num: number): string {
  if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`;
  if (num >= 1000) return `${(num / 1000).toFixed(0)}K`;
  return num.toString();
}

export default function HomePage() {
  // Fetch summary data
  const { data: enrollmentData, isLoading: enrollmentLoading } = useQuery({
    queryKey: ["enrollment-timeseries"],
    queryFn: () => enrollmentAPI.getTimeSeries(),
  });

  const { data: marketShareData } = useQuery({
    queryKey: ["market-share"],
    queryFn: () => enrollmentAPI.getMarketShare(),
  });

  const { data: bandData, isLoading: bandLoading } = useQuery({
    queryKey: ["stars-band"],
    queryFn: () => starsAPI.getByBand(),
  });

  const { data: yearsData } = useQuery({
    queryKey: ["years"],
    queryFn: lookupAPI.getYears,
  });

  const { data: parentsData } = useQuery({
    queryKey: ["parents"],
    queryFn: lookupAPI.getParents,
  });

  // Transform enrollment data
  const enrollmentChartData = enrollmentData
    ? enrollmentData.years.map((year, i) => ({
        year,
        enrollment: enrollmentData.total_enrollment[i],
      }))
    : [];

  // Transform star band data for pie
  const starPieData = bandData?.bands
    ? Object.entries(bandData.bands)
        .map(([star, count]) => ({
          name: `${star} Stars`,
          value: count as number,
          starNum: parseInt(star),
        }))
        .sort((a, b) => a.starNum - b.starNum)
    : [];

  const totalContracts = starPieData.reduce((sum, d) => sum + d.value, 0);
  const fourPlusStars = starPieData.filter(d => d.starNum >= 4).reduce((sum, d) => sum + d.value, 0);

  const latestEnrollment = enrollmentChartData.length > 0
    ? enrollmentChartData[enrollmentChartData.length - 1].enrollment
    : 0;

  const topPayers = marketShareData?.data?.slice(0, 5) || [];

  return (
    <div className="space-y-6">
      {/* Welcome Header */}
      <div className="bg-gradient-to-r from-blue-900 to-blue-700 rounded-lg shadow-lg p-8 text-white">
        <h1 className="text-3xl font-bold mb-2">Welcome to MA Intelligence Platform</h1>
        <p className="text-blue-100 text-lg">
          Comprehensive Medicare Advantage data analytics covering enrollment, Star ratings, and risk scores.
        </p>
      </div>

      {/* Key Stats Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="bg-white rounded-lg shadow p-6">
          <div className="flex items-center gap-3">
            <div className="p-3 bg-blue-100 rounded-lg">
              <Users className="w-6 h-6 text-blue-600" />
            </div>
            <div>
              <p className="text-sm text-gray-500">Total Enrollment</p>
              <p className="text-2xl font-bold text-gray-900">{formatNumber(latestEnrollment)}</p>
            </div>
          </div>
        </div>
        <div className="bg-white rounded-lg shadow p-6">
          <div className="flex items-center gap-3">
            <div className="p-3 bg-green-100 rounded-lg">
              <Building2 className="w-6 h-6 text-green-600" />
            </div>
            <div>
              <p className="text-sm text-gray-500">Parent Organizations</p>
              <p className="text-2xl font-bold text-gray-900">{parentsData?.count || "-"}</p>
            </div>
          </div>
        </div>
        <div className="bg-white rounded-lg shadow p-6">
          <div className="flex items-center gap-3">
            <div className="p-3 bg-yellow-100 rounded-lg">
              <Star className="w-6 h-6 text-yellow-600 fill-yellow-600" />
            </div>
            <div>
              <p className="text-sm text-gray-500">4+ Star Plans</p>
              <p className="text-2xl font-bold text-gray-900">
                {fourPlusStars} ({totalContracts > 0 ? ((fourPlusStars / totalContracts) * 100).toFixed(0) : 0}%)
              </p>
            </div>
          </div>
        </div>
        <div className="bg-white rounded-lg shadow p-6">
          <div className="flex items-center gap-3">
            <div className="p-3 bg-purple-100 rounded-lg">
              <Calendar className="w-6 h-6 text-purple-600" />
            </div>
            <div>
              <p className="text-sm text-gray-500">Years of Data</p>
              <p className="text-2xl font-bold text-gray-900">
                {yearsData?.enrollment_years?.length || "-"} years
              </p>
            </div>
          </div>
        </div>
      </div>

      {/* Charts Row */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Enrollment Trend */}
        <div className="bg-white rounded-lg shadow p-6">
          <div className="flex justify-between items-center mb-4">
            <h2 className="text-xl font-semibold">MA Enrollment Trend</h2>
            <Link
              href="/enrollment"
              className="text-blue-600 hover:text-blue-800 flex items-center gap-1 text-sm"
            >
              View Details <ArrowRight className="w-4 h-4" />
            </Link>
          </div>
          <div className="h-64">
            {enrollmentLoading ? (
              <div className="flex items-center justify-center h-full">
                <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
              </div>
            ) : (
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={enrollmentChartData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="year" />
                  <YAxis tickFormatter={formatNumber} />
                  <Tooltip formatter={(value) => [formatNumber(value as number), "Enrollment"]} />
                  <Line
                    type="monotone"
                    dataKey="enrollment"
                    stroke="#2563eb"
                    strokeWidth={2}
                    dot={{ fill: "#2563eb", r: 3 }}
                  />
                </LineChart>
              </ResponsiveContainer>
            )}
          </div>
        </div>

        {/* Star Rating Distribution */}
        <div className="bg-white rounded-lg shadow p-6">
          <div className="flex justify-between items-center mb-4">
            <h2 className="text-xl font-semibold">Star Rating Distribution</h2>
            <Link
              href="/stars"
              className="text-blue-600 hover:text-blue-800 flex items-center gap-1 text-sm"
            >
              View Details <ArrowRight className="w-4 h-4" />
            </Link>
          </div>
          <div className="h-64">
            {bandLoading ? (
              <div className="flex items-center justify-center h-full">
                <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
              </div>
            ) : (
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie
                    data={starPieData}
                    cx="50%"
                    cy="50%"
                    innerRadius={40}
                    outerRadius={80}
                    fill="#8884d8"
                    dataKey="value"
                    label={({ name, value }) => `${name}: ${value}`}
                  >
                    {starPieData.map((entry, index) => (
                      <Cell
                        key={`cell-${index}`}
                        fill={STAR_COLORS[entry.starNum.toString() as keyof typeof STAR_COLORS] || "#8884d8"}
                      />
                    ))}
                  </Pie>
                  <Tooltip />
                </PieChart>
              </ResponsiveContainer>
            )}
          </div>
        </div>
      </div>

      {/* Top Payers Table */}
      <div className="bg-white rounded-lg shadow p-6">
        <div className="flex justify-between items-center mb-4">
          <h2 className="text-xl font-semibold">Top Parent Organizations by Enrollment</h2>
          <Link
            href="/enrollment"
            className="text-blue-600 hover:text-blue-800 flex items-center gap-1 text-sm"
          >
            View All <ArrowRight className="w-4 h-4" />
          </Link>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className="border-b bg-gray-50">
                <th className="text-left py-3 px-4">Rank</th>
                <th className="text-left py-3 px-4">Parent Organization</th>
                <th className="text-right py-3 px-4">Enrollment</th>
                <th className="text-right py-3 px-4">Market Share</th>
                <th className="text-left py-3 px-4">Share</th>
              </tr>
            </thead>
            <tbody>
              {topPayers.map((payer, i) => (
                <tr key={i} className="border-b hover:bg-gray-50">
                  <td className="py-3 px-4 font-medium">{i + 1}</td>
                  <td className="py-3 px-4">{payer.parent_org}</td>
                  <td className="text-right py-3 px-4">{payer.total_enrollment.toLocaleString()}</td>
                  <td className="text-right py-3 px-4">{payer.market_share}%</td>
                  <td className="py-3 px-4">
                    <div className="w-32 bg-gray-200 rounded-full h-2.5">
                      <div
                        className="bg-blue-600 h-2.5 rounded-full"
                        style={{ width: `${Math.min(payer.market_share * 3, 100)}%` }}
                      ></div>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Quick Links */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <Link
          href="/enrollment"
          className="bg-white rounded-lg shadow p-6 hover:shadow-lg transition-shadow border-l-4 border-blue-600"
        >
          <div className="flex items-center gap-4">
            <Users className="w-10 h-10 text-blue-600" />
            <div>
              <h3 className="font-semibold text-lg">Enrollment Analysis</h3>
              <p className="text-gray-500 text-sm">View trends, market share, and payer breakdowns</p>
            </div>
          </div>
        </Link>
        <Link
          href="/stars"
          className="bg-white rounded-lg shadow p-6 hover:shadow-lg transition-shadow border-l-4 border-yellow-500"
        >
          <div className="flex items-center gap-4">
            <Star className="w-10 h-10 text-yellow-500" />
            <div>
              <h3 className="font-semibold text-lg">Star Ratings</h3>
              <p className="text-gray-500 text-sm">Analyze ratings, measures, and performance</p>
            </div>
          </div>
        </Link>
        <Link
          href="/risk-scores"
          className="bg-white rounded-lg shadow p-6 hover:shadow-lg transition-shadow border-l-4 border-green-600"
        >
          <div className="flex items-center gap-4">
            <TrendingUp className="w-10 h-10 text-green-600" />
            <div>
              <h3 className="font-semibold text-lg">Risk Scores</h3>
              <p className="text-gray-500 text-sm">Explore risk adjustment and distributions</p>
            </div>
          </div>
        </Link>
      </div>
    </div>
  );
}

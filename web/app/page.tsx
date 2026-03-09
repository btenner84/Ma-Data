"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";

export default function HomePage() {
  const router = useRouter();
  
  useEffect(() => {
    router.replace("/enrollment");
  }, [router]);
  
  return (
    <div className="min-h-screen flex items-center justify-center">
      <div className="text-gray-500">Redirecting to Enrollment...</div>
    </div>
  );
}

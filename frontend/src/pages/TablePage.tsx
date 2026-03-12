import { LeadsTable } from "@/components/LeadsTable";
import { useAuth } from "@/auth/AuthContext";

export function TablePage() {
  const { isAuthenticated } = useAuth();

  if (!isAuthenticated) {
    return (
      <div>
        <h1>Table Page</h1>Please log in to view the table.
      </div>
    );
  }
  return (
    <div className="table-page">
      <h1>Table Page</h1>
      <LeadsTable />
    </div>
  );
}

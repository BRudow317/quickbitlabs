import math

def run_pay_calc():
    principal = get_principal()
    interest_rate = get_interest_rate()
    payment_amount = get_payment()
    
    interest_total = 0.0
    total_paid = 0.0
    counter = 1
    
    print(
        f"\n--- LOAN DETAILS ---\n"
        f"Starting Principal: ${principal:,.2f}\n"
        f"Interest Rate: {interest_rate}%\n"
        f"Monthly Payment: ${payment_amount:,.2f}\n"
        f"--------------------\n"
    )
    
    # FIX: Loop while money is still owed. 
    # Added a safety break at 1000 months just in case payment is too low to cover interest.
    while principal > 0:
        if counter > 1000:
            print("(!) Payment too small to cover interest. Infinite loop stopped.")
            break

        # Calculate interest for this month
        monthly_interest = calc_interest_monthly(interest_rate, principal)
        
        # Calculate how much principal we CAN pay
        if principal + monthly_interest <= payment_amount:
            # Final payment case: Pay off the rest
            payment_this_month = principal + monthly_interest
            principal_payment = principal
            principal = 0
        else:
            # Normal payment case
            payment_this_month = payment_amount
            principal_payment = payment_amount - monthly_interest
            principal -= principal_payment

        interest_total += monthly_interest
        total_paid += payment_this_month
        
        output = (
            f"Month #{counter:<3} | "
            f"Princ Paid: ${principal_payment:>8.2f} | "
            f"Int Paid: ${monthly_interest:>8.2f} | "
            f"Rem. Princ: ${principal:>10.2f}"
        )
        print(output)
        
        counter += 1
    
    print("\n--- SUMMARY ---")
    print(f"Total Months: {counter - 1}")
    print(f"Total Paid: ${total_paid:,.2f}")
    print(f"Total Interest: ${interest_total:,.2f}")


def get_principal():
    return float(input("Input Principal: "))

def get_interest_rate():
    return float(input("Input Annual Interest Rate (%): "))

def get_payment():
    return float(input("Input Monthly Payment: "))

def calc_interest_monthly(annual_rate, current_principal):
    # Interest = (Rate / 100 / 12 months) * Principal
    return round((annual_rate / 100.0 / 12.0) * current_principal, 2)

if __name__ == "__main__":
    run_pay_calc()
def get_fahr_from_celsius(temp):
    return (temp * 9/5) + 32


def get_celsius_from_fahr(temp):
    return (temp - 32) * 5/9


def calculate_index(T, RH):
    # T: Temperature in Fahrenheit
    # RH: Air Humidity

    HI_S = (1.1 * T) - 10.3 + (0.047 * RH)

    if HI_S < 80:
        HI = HI_S
        return HI

    HI_S = -42.379 + (2.04901523 * T) + (10.14333127 * RH) - (0.22475541 * T * RH) - \
        (6.83783 * 10**-3 * T**2) - (5.481717 * 10**-2 * RH**2) + (1.22874 * 10**-3 * T**2 * RH) + \
        (8.5282 * 10**-4 * T * RH**2) - (1.99 * 10**-6 * T**-6 * T**2 * RH**2)

    if (80 <= T and T <= 112) and RH <= 13:
        HI = HI_S - ((3.25 - (0.25 * RH)) * ((17 - abs(T - 95))/17)**0.5)
        return HI

    if (80 <= T and T <= 87) and RH > 85:
        HI = HI_S + (0.02 * (RH - 85) * (87 * T))
        return HI

    HI = HI_S
    return HI


if __name__ == "__main__":
    T = get_fahr_from_celsius(30.4)
    index = calculate_index(T, 5.9)
    print("index: " + str(index))
    print(get_celsius_from_fahr(index))
